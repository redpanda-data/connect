import {
	Project,
	SyntaxKind,
	type Node,
	type TypeNode,
	type TypeAliasDeclaration,
	type SourceFile,
	type PropertySignature,
	type VariableDeclaration,
} from "ts-morph";
import { parse, stringify } from "yaml";
import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

const SCHEMA_DIR = import.meta.dir;
const DIR = join(SCHEMA_DIR, "..");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Schema = Record<string, unknown>;

interface EndpointMeta {
	constName: string;
	method: string;
	pathTemplate: string; // e.g. "users/${p.user_id}" from TS source
	openApiPath: string; // e.g. "/v1/users/{user_id}"
	responseTypeName: string;
	paramsTypeName: string;
	bodyParams: string[];
	pathParams: string[];
	queryParams: string[];
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

const knownTypes = new Map<string, TypeAliasDeclaration>();
const schemas = new Map<string, Schema>();
const pendingTypes = new Set<string>();
const processingTypes = new Set<string>(); // cycle guard

// Simple type aliases that should be inlined
const INLINE_ALIASES: Record<string, Schema> = {
	IdRequest: { type: "string" },
	IdResponse: { type: "string" },
	TextRequest: { type: "string" },
	StringRequest: { type: "string" },
	EmojiRequest: { type: "string" },
	EmptyObject: { type: "object" },
};

// ---------------------------------------------------------------------------
// 1. Parse TS source
// ---------------------------------------------------------------------------

function loadTsSource(): SourceFile {
	const project = new Project({
		compilerOptions: { strict: true, target: 99 /* ESNext */ },
		useInMemoryFileSystem: false,
		skipAddingFilesFromTsConfig: true,
	});
	return project.addSourceFileAtPath(join(SCHEMA_DIR, "api-endpoints.ts"));
}

function collectTypeAliases(sf: SourceFile) {
	for (const ta of sf.getTypeAliases()) {
		knownTypes.set(ta.getName(), ta);
	}
	console.log(`  Collected ${knownTypes.size} type aliases`);
}

// ---------------------------------------------------------------------------
// 2. Extract endpoint metadata from `export const xxx = { ... } as const`
// ---------------------------------------------------------------------------

function extractEndpoints(sf: SourceFile): EndpointMeta[] {
	const endpoints: EndpointMeta[] = [];

	for (const vs of sf.getVariableStatements()) {
		if (!vs.hasExportKeyword()) continue;
		for (const decl of vs.getDeclarations()) {
			const init = decl.getInitializerIfKind(SyntaxKind.AsExpression);
			if (!init) continue;
			const obj = init.getExpressionIfKind(
				SyntaxKind.ObjectLiteralExpression,
			);
			if (!obj) continue;

			const methodProp = obj.getProperty("method");
			const pathProp = obj.getProperty("path");
			const bodyParamsProp = obj.getProperty("bodyParams");
			const pathParamsProp = obj.getProperty("pathParams");
			const queryParamsProp = obj.getProperty("queryParams");

			if (!methodProp || !pathProp) continue;

			const constName = decl.getName();
			const method = extractStringLiteral(methodProp);
			const pathTemplate = extractPathTemplate(pathProp);
			const bodyParams = extractStringArray(bodyParamsProp);
			const pathParams = extractStringArray(pathParamsProp);
			const queryParams = extractStringArray(queryParamsProp);

			if (!method || !pathTemplate) continue;

			// Derive type names by convention: getUser -> GetUserResponse
			const pascal =
				constName.charAt(0).toUpperCase() + constName.slice(1);
			const responseTypeName = `${pascal}Response`;
			const paramsTypeName = `${pascal}Parameters`;

			// Convert TS template to OpenAPI path:
			// "users/${p.user_id}" -> "/v1/users/{user_id}"
			const openApiPath =
				"/v1/" + pathTemplate.replace(/\$\{p\.(\w+)\}/g, "{$1}");

			endpoints.push({
				constName,
				method,
				pathTemplate,
				openApiPath,
				responseTypeName,
				paramsTypeName,
				bodyParams,
				pathParams,
				queryParams,
			});
		}
	}

	return endpoints;
}

function extractStringLiteral(node: Node): string | null {
	const init = (node as any).getInitializerIfKind?.(SyntaxKind.StringLiteral);
	return init?.getLiteralValue() ?? null;
}

function extractPathTemplate(node: Node): string | null {
	// path property is an arrow function with a template literal body
	const text = node.getText();
	const match = text.match(/`([^`]+)`/);
	return match ? match[1] : null;
}

function extractStringArray(node: Node | undefined): string[] {
	if (!node) return [];
	const init = (node as any).getInitializerIfKind?.(
		SyntaxKind.ArrayLiteralExpression,
	);
	if (!init) return [];
	return init
		.getElements()
		.map((e: any) => e.getLiteralValue?.())
		.filter(Boolean);
}

// ---------------------------------------------------------------------------
// 3. Match TS endpoints to OpenAPI operations
// ---------------------------------------------------------------------------

interface OpenApiOperation {
	path: string;
	method: string;
	operationObj: any;
}

function extractOpenApiOperations(spec: any): OpenApiOperation[] {
	const ops: OpenApiOperation[] = [];
	const paths = spec.paths || {};
	for (const [path, methods] of Object.entries(paths)) {
		for (const [method, operationObj] of Object.entries(
			methods as Record<string, any>,
		)) {
			if (["get", "post", "put", "patch", "delete"].includes(method)) {
				ops.push({ path, method, operationObj });
			}
		}
	}
	return ops;
}

function normalizePath(p: string): string {
	// Replace any {param_name} with * for fuzzy matching
	return p.replace(/\{[^}]+\}/g, "*").replace(/\/+$/, "");
}

function matchEndpoints(
	endpoints: EndpointMeta[],
	operations: OpenApiOperation[],
): Map<string, EndpointMeta> {
	// Key: "METHOD /path" from OpenAPI
	const matched = new Map<string, EndpointMeta>();

	// Manual overrides for path mismatches between Postman and TS SDK
	const overrides: Record<string, string> = {
		// Postman uses databases/{id}/query, TS SDK uses data_sources/{id}/query
		"post /v1/databases/*/query": "queryDataSource",
	};

	// Build lookup: normalized TS path -> EndpointMeta
	const tsLookup = new Map<string, EndpointMeta>();
	for (const ep of endpoints) {
		const key = `${ep.method} ${normalizePath(ep.openApiPath)}`;
		tsLookup.set(key, ep);
	}

	for (const op of operations) {
		const key = `${op.method} ${normalizePath(op.path)}`;
		const specKey = `${op.method} ${op.path}`;

		// Check manual overrides first
		const overrideName = overrides[key];
		if (overrideName) {
			const ep = endpoints.find((e) => e.constName === overrideName);
			if (ep) {
				matched.set(specKey, ep);
				continue;
			}
		}

		// Automatic match
		const ep = tsLookup.get(key);
		if (ep) {
			matched.set(specKey, ep);
		}
	}

	return matched;
}

// ---------------------------------------------------------------------------
// 4. TypeScript type -> OpenAPI schema conversion
// ---------------------------------------------------------------------------

function convertTypeNode(node: TypeNode): Schema {
	const kind = node.getKind();

	switch (kind) {
		case SyntaxKind.StringKeyword:
			return { type: "string" };
		case SyntaxKind.NumberKeyword:
			return { type: "number" };
		case SyntaxKind.BooleanKeyword:
			return { type: "boolean" };
		case SyntaxKind.NullKeyword:
			// Standalone null - caller handles nullable
			return { nullable: true };
		case SyntaxKind.UndefinedKeyword:
		case SyntaxKind.VoidKeyword:
		case SyntaxKind.NeverKeyword:
			return {};

		case SyntaxKind.LiteralType:
			return convertLiteralType(node);

		case SyntaxKind.TypeReference:
			return convertTypeReference(node);

		case SyntaxKind.TypeLiteral:
			return convertTypeLiteral(node);

		case SyntaxKind.UnionType:
			return convertUnionType(node);

		case SyntaxKind.IntersectionType:
			return convertIntersectionType(node);

		case SyntaxKind.ArrayType:
			return {
				type: "array",
				items: convertTypeNode((node as any).getElementTypeNode()),
			};

		case SyntaxKind.ParenthesizedType:
			return convertTypeNode((node as any).getTypeNode());

		case SyntaxKind.TupleType:
			return { type: "array" };

		case SyntaxKind.TypeQuery:
		case SyntaxKind.IndexedAccessType:
		case SyntaxKind.MappedType:
		case SyntaxKind.ConditionalType:
			// Fall back to type checker for complex types
			return convertViaTypeChecker(node);

		default:
			console.warn(
				`  [warn] Unhandled TypeNode kind: ${node.getKindName()}`,
			);
			return {};
	}
}

function convertLiteralType(node: TypeNode): Schema {
	const text = node.getText().trim();

	// String literal: "foo"
	if (text.startsWith('"') || text.startsWith("'")) {
		const val = text.slice(1, -1);
		return { type: "string", enum: [val] };
	}
	// Numeric literal
	if (/^-?\d+(\.\d+)?$/.test(text)) {
		return { type: "number", enum: [Number(text)] };
	}
	// Boolean literal
	if (text === "true") return { type: "boolean", enum: [true] };
	if (text === "false") return { type: "boolean", enum: [false] };
	// null literal
	if (text === "null") return { nullable: true };

	return { type: "string" };
}

function convertTypeReference(node: TypeNode): Schema {
	const refNode = node as any;
	const typeName = refNode.getTypeName?.()?.getText?.() ?? "";
	const typeArgs = refNode.getTypeArguments?.() ?? [];

	// Handle built-in generics
	if (typeName === "Array" && typeArgs.length === 1) {
		return { type: "array", items: convertTypeNode(typeArgs[0]) };
	}

	if (typeName === "Record" && typeArgs.length === 2) {
		const valSchema = convertTypeNode(typeArgs[1]);
		// Record<string, never> = empty object
		if (typeArgs[1].getKind() === SyntaxKind.NeverKeyword) {
			return { type: "object" };
		}
		return { type: "object", additionalProperties: valSchema };
	}

	if (typeName === "Omit" || typeName === "Pick" || typeName === "Partial") {
		return convertViaTypeChecker(node);
	}

	// Check inline aliases
	if (INLINE_ALIASES[typeName]) {
		return { ...INLINE_ALIASES[typeName] };
	}

	// Known type from the file -> $ref
	if (knownTypes.has(typeName)) {
		if (!schemas.has(typeName) && !processingTypes.has(typeName)) {
			pendingTypes.add(typeName);
		}
		return { $ref: `#/components/schemas/${typeName}` };
	}

	// Unknown type - try type checker
	return convertViaTypeChecker(node);
}

function convertTypeLiteral(node: TypeNode): Schema {
	const lit = node as any;
	const members = lit.getMembers?.() ?? [];
	const properties: Record<string, Schema> = {};
	const required: string[] = [];

	for (const member of members) {
		if (member.getKind() === SyntaxKind.PropertySignature) {
			const ps = member as PropertySignature;
			const name = ps.getName();
			const typeNode = ps.getTypeNode();
			if (typeNode) {
				const schema = convertTypeNode(typeNode);
				// Extract comment as description
				const jsDocs = ps
					.getLeadingCommentRanges()
					.map((c) => c.getText())
					.join(" ");
				const lineComment = jsDocs
					.replace(/^\/\/\s*/, "")
					.replace(/^\/\*\s*/, "")
					.replace(/\s*\*\/$/, "")
					.trim();
				if (lineComment) {
					schema.description = lineComment;
				}
				properties[name] = schema;
			}
			if (!ps.hasQuestionToken()) {
				required.push(name);
			}
		} else if (member.getKind() === SyntaxKind.IndexSignature) {
			// [key: string]: T -> additionalProperties
			const idxSig = member as any;
			const valType = idxSig.getReturnTypeNode?.();
			if (valType) {
				return {
					type: "object",
					additionalProperties: convertTypeNode(valType),
				};
			}
		}
	}

	const schema: Schema = { type: "object", properties };
	if (required.length > 0) schema.required = required;
	return schema;
}

function convertUnionType(node: TypeNode): Schema {
	const union = node as any;
	const typeNodes: TypeNode[] = union.getTypeNodes?.() ?? [];

	if (typeNodes.length === 0) return {};

	// Separate null members from non-null members
	const nullMembers = typeNodes.filter(
		(t) =>
			t.getKind() === SyntaxKind.NullKeyword ||
			(t.getKind() === SyntaxKind.LiteralType &&
				t.getText().trim() === "null"),
	);
	const nonNullMembers = typeNodes.filter(
		(t) =>
			t.getKind() !== SyntaxKind.NullKeyword &&
			!(
				t.getKind() === SyntaxKind.LiteralType &&
				t.getText().trim() === "null"
			),
	);

	const isNullable = nullMembers.length > 0;

	// If only null remains
	if (nonNullMembers.length === 0) return { nullable: true };

	// If exactly one non-null member + null -> nullable
	if (nonNullMembers.length === 1 && isNullable) {
		const schema = convertTypeNode(nonNullMembers[0]);
		schema.nullable = true;
		return schema;
	}

	// Check if all non-null members are string literals -> enum
	const allStringLiterals = nonNullMembers.every((t) => {
		const text = t.getText().trim();
		return (
			t.getKind() === SyntaxKind.LiteralType &&
			(text.startsWith('"') || text.startsWith("'"))
		);
	});
	if (allStringLiterals) {
		const values = nonNullMembers.map((t) => {
			const text = t.getText().trim();
			return text.slice(1, -1);
		});
		const schema: Schema = { type: "string", enum: values };
		if (isNullable) schema.nullable = true;
		return schema;
	}

	// Check if all non-null members are number literals -> enum
	const allNumberLiterals = nonNullMembers.every((t) => {
		const text = t.getText().trim();
		return (
			t.getKind() === SyntaxKind.LiteralType &&
			/^-?\d+(\.\d+)?$/.test(text)
		);
	});
	if (allNumberLiterals) {
		const values = nonNullMembers.map((t) => Number(t.getText().trim()));
		const schema: Schema = { type: "number", enum: values };
		if (isNullable) schema.nullable = true;
		return schema;
	}

	// General union -> oneOf
	// Try to detect discriminator (shared 'type' property with literal values)
	const converted = nonNullMembers.map((t) => convertTypeNode(t));
	const schema: Schema = { oneOf: converted };

	// Try to detect discriminator
	const discriminator = detectDiscriminator(nonNullMembers);
	if (discriminator) {
		schema.discriminator = { propertyName: discriminator };
	}

	if (isNullable) schema.nullable = true;
	return schema;
}

function detectDiscriminator(members: TypeNode[]): string | null {
	// Check if all members are object types with a 'type' property that has a string literal value
	for (const propName of ["type", "object"]) {
		let allHaveDiscriminator = true;
		for (const member of members) {
			if (member.getKind() !== SyntaxKind.TypeLiteral) {
				// If it's a type reference, we can't easily check without resolving
				allHaveDiscriminator = false;
				break;
			}
			const lit = member as any;
			const props = lit.getMembers?.() ?? [];
			const typeProp = props.find(
				(p: any) =>
					p.getKind() === SyntaxKind.PropertySignature &&
					p.getName() === propName,
			);
			if (!typeProp) {
				allHaveDiscriminator = false;
				break;
			}
			const typeNode = (typeProp as PropertySignature).getTypeNode();
			if (!typeNode) {
				allHaveDiscriminator = false;
				break;
			}
			// Must be a string literal type
			const text = typeNode.getText().trim();
			if (
				!(
					typeNode.getKind() === SyntaxKind.LiteralType &&
					(text.startsWith('"') || text.startsWith("'"))
				)
			) {
				allHaveDiscriminator = false;
				break;
			}
		}
		if (allHaveDiscriminator && members.length > 1) return propName;
	}
	return null;
}

function convertIntersectionType(node: TypeNode): Schema {
	const inter = node as any;
	const typeNodes: TypeNode[] = inter.getTypeNodes?.() ?? [];

	if (typeNodes.length === 0) return {};
	if (typeNodes.length === 1) return convertTypeNode(typeNodes[0]);

	return { allOf: typeNodes.map((t) => convertTypeNode(t)) };
}

function convertViaTypeChecker(node: TypeNode): Schema {
	// Use ts-morph type checker to get the resolved type
	try {
		const type = node.getType();

		if (type.isString()) return { type: "string" };
		if (type.isNumber()) return { type: "number" };
		if (type.isBoolean()) return { type: "boolean" };
		if (type.isNull()) return { nullable: true };
		if (type.isUndefined()) return {};

		if (type.isStringLiteral())
			return {
				type: "string",
				enum: [type.getLiteralValue() as string],
			};
		if (type.isNumberLiteral())
			return {
				type: "number",
				enum: [type.getLiteralValue() as number],
			};
		if (type.isBooleanLiteral())
			return {
				type: "boolean",
				enum: [type.getLiteralValue() as boolean],
			};

		if (type.isArray()) {
			const elemType = type.getArrayElementTypeOrThrow();
			// Can't easily convert resolved element types back to AST
			return { type: "array" };
		}

		if (type.isObject()) {
			const props = type.getProperties();
			if (props.length === 0) return { type: "object" };

			const properties: Record<string, Schema> = {};
			const required: string[] = [];
			for (const prop of props) {
				const propType = prop.getValueDeclaration()?.getType();
				if (propType) {
					if (propType.isString())
						properties[prop.getName()] = { type: "string" };
					else if (propType.isNumber())
						properties[prop.getName()] = { type: "number" };
					else if (propType.isBoolean())
						properties[prop.getName()] = { type: "boolean" };
					else properties[prop.getName()] = {};
				} else {
					properties[prop.getName()] = {};
				}
				if (!prop.isOptional()) required.push(prop.getName());
			}
			const schema: Schema = { type: "object", properties };
			if (required.length > 0) schema.required = required;
			return schema;
		}
	} catch {
		// fallback
	}

	return {};
}

// ---------------------------------------------------------------------------
// 5. Process pending types breadth-first
// ---------------------------------------------------------------------------

function processAllPendingTypes() {
	let iterations = 0;
	const maxIterations = 2000;

	while (pendingTypes.size > 0 && iterations < maxIterations) {
		iterations++;
		const name = pendingTypes.values().next().value!;
		pendingTypes.delete(name);

		if (schemas.has(name) || processingTypes.has(name)) continue;
		if (INLINE_ALIASES[name]) continue;

		const ta = knownTypes.get(name);
		if (!ta) {
			console.warn(`  [warn] Type not found: ${name}`);
			continue;
		}

		processingTypes.add(name);
		const typeNode = ta.getTypeNode();
		if (typeNode) {
			schemas.set(name, convertTypeNode(typeNode));
		}
		processingTypes.delete(name);
	}

	if (iterations >= maxIterations) {
		console.warn(
			`  [warn] Hit max iterations (${maxIterations}), ${pendingTypes.size} types remaining`,
		);
	}
}

// ---------------------------------------------------------------------------
// 6. Build request body schema from Parameters type
// ---------------------------------------------------------------------------

function buildRequestBodySchema(ep: EndpointMeta): Schema | null {
	if (ep.bodyParams.length === 0) return null;

	const paramsType = knownTypes.get(ep.paramsTypeName);
	if (!paramsType) return null;

	// The Parameters type may include path + query + body params
	// We need to extract only the body params
	// Strategy: look for the Body-specific type (e.g., CreatePageBodyParameters)
	const bodyTypeName = `${ep.constName.charAt(0).toUpperCase() + ep.constName.slice(1)}BodyParameters`;
	const bodyType = knownTypes.get(bodyTypeName);

	if (bodyType) {
		if (!schemas.has(bodyTypeName) && !processingTypes.has(bodyTypeName)) {
			pendingTypes.add(bodyTypeName);
		}
		return { $ref: `#/components/schemas/${bodyTypeName}` };
	}

	// Fall back to the full Parameters type
	if (
		!schemas.has(ep.paramsTypeName) &&
		!processingTypes.has(ep.paramsTypeName)
	) {
		pendingTypes.add(ep.paramsTypeName);
	}
	return { $ref: `#/components/schemas/${ep.paramsTypeName}` };
}

// ---------------------------------------------------------------------------
// 7. Post-process schemas for ogen compatibility
// ---------------------------------------------------------------------------

function postProcessSchemas() {
	let flattenCount = 0;
	let mappingCount = 0;

	// Pass 1: Flatten allOf+oneOf patterns
	// When allOf([ref(Common), { oneOf: [ref(A), ref(B)] }]):
	//   If variants are $refs (named types that already include common props) → just oneOf([ref(A), ref(B)])
	//   If variants are inline → create merged schemas
	for (const [name, schema] of schemas) {
		const allOf = schema.allOf as Schema[] | undefined;
		if (!allOf || allOf.length !== 2) continue;

		// Find the common part and the oneOf part
		let commonIdx = -1;
		let oneOfIdx = -1;
		for (let i = 0; i < allOf.length; i++) {
			if (allOf[i].oneOf) oneOfIdx = i;
			else commonIdx = i;
		}
		if (commonIdx === -1 || oneOfIdx === -1) continue;

		const commonPart = allOf[commonIdx];
		const oneOfPart = allOf[oneOfIdx];
		const variants = oneOfPart.oneOf as Schema[];
		if (!variants || variants.length === 0) continue;

		// Merge common + all variants into a single flat object.
		// Since ogen can't discriminate object oneOf variants, we merge everything.
		const commonResolved = resolveToObject(commonPart);
		const commonProps =
			(commonResolved?.properties as Record<string, Schema>) || {};
		const commonReq = new Set((commonResolved?.required as string[]) || []);

		const allProperties: Record<string, Schema> = { ...commonProps };
		const variantRequiredSets: Set<string>[] = [];

		for (const variant of variants) {
			const resolved = resolveToObject(variant);
			if (!resolved) continue;
			const vProps =
				(resolved.properties as Record<string, Schema>) || {};
			for (const [k, v] of Object.entries(vProps)) {
				if (!allProperties[k]) {
					allProperties[k] = { ...v };
				} else if (v.enum && allProperties[k].enum) {
					// Merge enum values
					allProperties[k] = {
						...allProperties[k],
						enum: [
							...new Set([
								...(allProperties[k].enum as any[]),
								...(v.enum as any[]),
							]),
						],
					};
				}
			}
			variantRequiredSets.push(
				new Set((resolved.required as string[]) || []),
			);
		}

		// Required: common required + intersection of variant required
		const variantRequired =
			variantRequiredSets.length > 0
				? [...variantRequiredSets[0]].filter((f) =>
						variantRequiredSets.every((s) => s.has(f)),
					)
				: [];
		const allRequired = [...new Set([...commonReq, ...variantRequired])];

		delete schema.allOf;
		delete schema.oneOf;
		delete schema.discriminator;
		schema.type = "object";
		schema.properties = allProperties;
		if (allRequired.length > 0) schema.required = allRequired;

		// Carry over discriminator and nullable
		if (oneOfPart.discriminator) {
			schema.discriminator = oneOfPart.discriminator;
		}
		if (oneOfPart.nullable) {
			schema.nullable = oneOfPart.nullable;
		}

		flattenCount++;
	}

	// Pass 1b: Flatten remaining allOf with only $ref members (non-oneOf cases)
	for (const [name, schema] of schemas) {
		const allOf = schema.allOf as Schema[] | undefined;
		if (!allOf) continue;

		// Skip if already handled (has oneOf)
		if (schema.oneOf) continue;

		const allRefs = allOf.every((m) => m.$ref);
		if (allRefs) {
			const merged = mergeAllOfRefs(allOf);
			if (merged) {
				delete schema.allOf;
				Object.assign(schema, merged);
				flattenCount++;
			}
		}
	}

	// Pass 2: Detect discriminators and add mappings for ALL oneOf schemas
	for (const [name, schema] of schemas) {
		detectAndMapDiscriminator(schema);
	}

	// Pass 3: Recursively merge all oneOf/allOf in the entire schema tree.
	// ogen can't discriminate oneOf when all members are JSON objects.
	let mergedOneOfCount = 0;
	const visited = new Set<Schema>();

	// Run until convergence (merged properties may contain new oneOf)
	let prevCount = -1;
	while (mergedOneOfCount !== prevCount) {
		prevCount = mergedOneOfCount;
		visited.clear();
		for (const [name, schema] of schemas) {
			flattenSchemaRecursive(schema);
		}
	}

	function flattenSchemaRecursive(schema: Schema) {
		if (!schema || typeof schema !== "object") return;
		if (visited.has(schema)) return;
		visited.add(schema);

		// Flatten oneOf at this level
		if (
			schema.oneOf &&
			Array.isArray(schema.oneOf) &&
			(schema.oneOf as Schema[]).length >= 2
		) {
			const variants = schema.oneOf as Schema[];
			const merged = mergeOneOfVariants("inline", variants);
			if (merged) {
				const disc = schema.discriminator as
					| { propertyName: string; mapping?: Record<string, string> }
					| undefined;
				if (disc?.mapping) {
					const mergedProps = merged.properties as Record<
						string,
						Schema
					>;
					if (mergedProps[disc.propertyName]) {
						mergedProps[disc.propertyName] = {
							type: "string",
							enum: Object.keys(disc.mapping),
						};
					}
				}
				if (schema.nullable) merged.nullable = true;
				delete schema.oneOf;
				delete schema.discriminator;
				Object.assign(schema, merged);
				mergedOneOfCount++;
			}
		}

		// Flatten allOf at this level
		if (schema.allOf && Array.isArray(schema.allOf)) {
			const allOf = schema.allOf as Schema[];
			const merged = mergeAllOfParts(allOf);
			if (merged) {
				if (schema.nullable) merged.nullable = true;
				delete schema.allOf;
				Object.assign(schema, merged);
				flattenCount++;
			}
		}

		// Recurse into properties
		if (schema.properties && typeof schema.properties === "object") {
			for (const val of Object.values(
				schema.properties as Record<string, Schema>,
			)) {
				flattenSchemaRecursive(val);
			}
		}
		// Recurse into items
		if (schema.items && typeof schema.items === "object") {
			flattenSchemaRecursive(schema.items as Schema);
		}
		// Recurse into additionalProperties
		if (
			schema.additionalProperties &&
			typeof schema.additionalProperties === "object"
		) {
			flattenSchemaRecursive(schema.additionalProperties as Schema);
		}
	}

	function mergeAllOfParts(allOf: Schema[]): Schema | null {
		const merged: Record<string, Schema> = {};
		const required: string[] = [];

		for (const part of allOf) {
			const resolved = resolveToObject(part);
			if (!resolved) return null;
			const props = (resolved.properties as Record<string, Schema>) || {};
			Object.assign(merged, props);
			const req = (resolved.required as string[]) || [];
			required.push(...req);
		}

		return {
			type: "object",
			properties: merged,
			...(required.length > 0
				? { required: [...new Set(required)] }
				: {}),
		};
	}

	console.log(
		`  Flattened ${flattenCount} allOf patterns, added ${mappingCount} discriminator mappings, merged ${mergedOneOfCount} oneOf into flat objects`,
	);

	function mergeOneOfVariants(
		parentName: string,
		variants: Schema[],
	): Schema | null {
		const allProperties: Record<string, Schema> = {};
		const requiredSets: Set<string>[] = [];
		let allResolvable = true;

		for (const variant of variants) {
			const resolved = resolveToObject(variant);
			if (!resolved) {
				allResolvable = false;
				break;
			}

			const props = resolved.properties as
				| Record<string, Schema>
				| undefined;
			if (props) {
				for (const [k, v] of Object.entries(props)) {
					// Keep the first definition, or merge enums
					if (!allProperties[k]) {
						allProperties[k] = { ...v };
					} else {
						// If both have enum, merge the enum values
						const existing = allProperties[k];
						if (existing.enum && v.enum) {
							const merged = [
								...new Set([
									...(existing.enum as any[]),
									...(v.enum as any[]),
								]),
							];
							existing.enum = merged;
						}
						// If one has nullable, propagate it
						if (v.nullable) existing.nullable = true;
					}
				}
			}

			const req = resolved.required as string[] | undefined;
			requiredSets.push(new Set(req || []));
		}

		if (!allResolvable) return null;

		// Required = intersection (fields required in ALL variants)
		let required: string[] = [];
		if (requiredSets.length > 0) {
			required = [...requiredSets[0]].filter((field) =>
				requiredSets.every((s) => s.has(field)),
			);
		}

		const result: Schema = {
			type: "object",
			properties: allProperties,
		};
		if (required.length > 0) result.required = required;
		return result;
	}

	function resolveToObject(schema: Schema): Schema | null {
		// Direct object (with or without properties)
		if (schema.type === "object") {
			return { ...schema, properties: schema.properties || {} };
		}

		// $ref
		if (schema.$ref) {
			const refName = (schema.$ref as string).replace(
				"#/components/schemas/",
				"",
			);
			const refSchema = schemas.get(refName);
			if (refSchema) return resolveToObject(refSchema);
			return null;
		}

		// allOf - merge all parts
		if (schema.allOf) {
			const allOf = schema.allOf as Schema[];
			const merged: Record<string, Schema> = {};
			const required: string[] = [];

			for (const part of allOf) {
				const resolved = resolveToObject(part);
				if (!resolved) return null;
				const props =
					(resolved.properties as Record<string, Schema>) || {};
				Object.assign(merged, props);
				const req = (resolved.required as string[]) || [];
				required.push(...req);
			}

			return {
				type: "object",
				properties: merged,
				...(required.length > 0
					? { required: [...new Set(required)] }
					: {}),
			};
		}

		// oneOf - recursively merge (for nested oneOf)
		if (schema.oneOf) {
			return mergeOneOfVariants("nested", schema.oneOf as Schema[]);
		}

		// Can't resolve
		return null;
	}

	function detectAndMapDiscriminator(schema: Schema) {
		const variants = schema.oneOf as Schema[] | undefined;
		if (!variants || variants.length < 2) return;

		// Try common discriminator property names
		for (const propName of ["type", "object"]) {
			const mapping: Record<string, string> = {};
			let allHaveValue = true;

			for (const variant of variants) {
				const info = resolveVariantInfo(variant, propName);
				if (!info.value) {
					allHaveValue = false;
					break;
				}
				mapping[info.value] = info.ref || "";
			}

			if (
				allHaveValue &&
				Object.keys(mapping).length === variants.length
			) {
				// All variants have unique discriminator values
				// Only add mapping if we have refs
				const hasRefs = Object.values(mapping).every(
					(v) => v.length > 0,
				);
				schema.discriminator = {
					propertyName: propName,
					...(hasRefs ? { mapping } : {}),
				};
				mappingCount++;
				return;
			}
		}
	}

	function resolveVariantInfo(
		variant: Schema,
		propName: string,
	): { value: string | null; ref: string | null } {
		// Case 1: Direct $ref
		if (variant.$ref) {
			const ref = variant.$ref as string;
			const refName = ref.replace("#/components/schemas/", "");
			const refSchema = schemas.get(refName);
			if (!refSchema) return { value: null, ref };
			return {
				value: extractDiscriminatorValue(refSchema, propName),
				ref,
			};
		}

		// Case 2: allOf (from flattening pass)
		if (variant.allOf) {
			const allOf = variant.allOf as Schema[];
			// Collect the discriminator value from all parts
			for (const part of allOf) {
				if (part.$ref) {
					const refName = (part.$ref as string).replace(
						"#/components/schemas/",
						"",
					);
					const refSchema = schemas.get(refName);
					if (refSchema) {
						const val = extractDiscriminatorValue(
							refSchema,
							propName,
						);
						if (val) {
							// Use the non-common ref as the mapping target
							return { value: val, ref: part.$ref as string };
						}
					}
				}
				// Inline object with the discriminator
				const props = part.properties as
					| Record<string, Schema>
					| undefined;
				if (props?.[propName]) {
					const enumVals = props[propName].enum as
						| string[]
						| undefined;
					if (enumVals?.length === 1) {
						return { value: enumVals[0], ref: null };
					}
				}
			}
			return { value: null, ref: null };
		}

		// Case 3: Inline object
		const props = variant.properties as Record<string, Schema> | undefined;
		if (props?.[propName]) {
			const enumVals = props[propName].enum as string[] | undefined;
			if (enumVals?.length === 1) {
				return { value: enumVals[0], ref: null };
			}
		}

		return { value: null, ref: null };
	}

	function extractDiscriminatorValue(
		schema: Schema,
		propName: string,
	): string | null {
		// Direct properties
		const props = schema.properties as Record<string, Schema> | undefined;
		if (props?.[propName]) {
			const enumVals = props[propName].enum as string[] | undefined;
			if (enumVals?.length === 1) return enumVals[0];
		}

		// Check inside allOf
		const allOf = schema.allOf as Schema[] | undefined;
		if (allOf) {
			for (const part of allOf) {
				if (part.$ref) {
					const refName = (part.$ref as string).replace(
						"#/components/schemas/",
						"",
					);
					const refSchema = schemas.get(refName);
					if (refSchema) {
						const val = extractDiscriminatorValue(
							refSchema,
							propName,
						);
						if (val) return val;
					}
				}
				const partProps = part.properties as
					| Record<string, Schema>
					| undefined;
				if (partProps?.[propName]) {
					const enumVals = partProps[propName].enum as
						| string[]
						| undefined;
					if (enumVals?.length === 1) return enumVals[0];
				}
			}
		}

		return null;
	}

	function mergeAllOfRefs(allOf: Schema[]): Schema | null {
		const merged: Schema = {
			type: "object",
			properties: {} as Record<string, Schema>,
		};
		const required: string[] = [];

		for (const part of allOf) {
			const refName = (part.$ref as string)?.replace(
				"#/components/schemas/",
				"",
			);
			if (!refName) return null;

			const refSchema = schemas.get(refName);
			if (!refSchema) return null;

			const props = refSchema.properties as
				| Record<string, Schema>
				| undefined;
			if (props) {
				Object.assign(
					merged.properties as Record<string, Schema>,
					props,
				);
			}
			const req = refSchema.required as string[] | undefined;
			if (req) {
				required.push(...req);
			}
		}

		if (required.length > 0) {
			merged.required = [...new Set(required)];
		}

		return merged;
	}
}

// ---------------------------------------------------------------------------
// 8. Merge schemas into OpenAPI spec
// ---------------------------------------------------------------------------

function mergeIntoSpec(
	spec: any,
	operations: OpenApiOperation[],
	matched: Map<string, EndpointMeta>,
) {
	// Ensure components.schemas exists
	if (!spec.components) spec.components = {};
	if (!spec.components.schemas) spec.components.schemas = {};

	// First pass: queue all matched response/request types
	for (const [specKey, ep] of matched) {
		if (knownTypes.has(ep.responseTypeName)) {
			pendingTypes.add(ep.responseTypeName);
		}
		buildRequestBodySchema(ep); // queues body type if exists
	}

	// Process all pending types
	processAllPendingTypes();

	// Add all schemas to components
	for (const [name, schema] of schemas) {
		spec.components.schemas[name] = schema;
	}

	// Second pass: update operations with $refs
	const paths = spec.paths || {};
	for (const [path, methods] of Object.entries(paths)) {
		for (const [method, opObj] of Object.entries(
			methods as Record<string, any>,
		)) {
			if (!["get", "post", "put", "patch", "delete"].includes(method))
				continue;

			const specKey = `${method} ${path}`;
			const ep = matched.get(specKey);
			if (!ep) continue;

			// Update response schema
			if (knownTypes.has(ep.responseTypeName)) {
				const resp = opObj.responses?.["200"];
				if (resp) {
					if (!resp.content) resp.content = {};
					resp.content["application/json"] = {
						schema: {
							$ref: `#/components/schemas/${ep.responseTypeName}`,
						},
					};
				}
			}

			// Update request body schema
			if (ep.bodyParams.length > 0) {
				const bodySchema = buildRequestBodySchema(ep);
				if (bodySchema) {
					opObj.requestBody = {
						required: true,
						content: {
							"application/json": {
								schema: bodySchema,
							},
						},
					};
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
	console.log("Phase 1: Parse TypeScript source");
	const sf = loadTsSource();
	collectTypeAliases(sf);

	console.log("\nPhase 2: Extract endpoint metadata from TS SDK");
	const endpoints = extractEndpoints(sf);
	console.log(`  Found ${endpoints.length} endpoints:\n`);
	for (const ep of endpoints) {
		console.log(
			`  ${ep.method.toUpperCase().padEnd(6)} ${ep.openApiPath.padEnd(50)} ${ep.constName}`,
		);
	}

	console.log("\n\nPhase 3: Read OpenAPI spec and match endpoints");
	const specText = readFileSync(join(DIR, "openapi.yaml"), "utf-8");
	const spec = parse(specText, { maxAliasCount: 100000 });
	const operations = extractOpenApiOperations(spec);

	console.log(`  OpenAPI operations:\n`);
	for (const op of operations) {
		console.log(`  ${op.method.toUpperCase().padEnd(6)} ${op.path}`);
	}

	const matched = matchEndpoints(endpoints, operations);

	console.log(
		`\n  Matched ${matched.size} of ${operations.length} operations:\n`,
	);
	for (const op of operations) {
		const specKey = `${op.method} ${op.path}`;
		const ep = matched.get(specKey);
		const status = ep
			? `-> ${ep.constName} (${ep.responseTypeName})`
			: "[UNMATCHED]";
		console.log(
			`  ${op.method.toUpperCase().padEnd(6)} ${op.path.padEnd(55)} ${status}`,
		);
	}

	const unmatched = operations.filter(
		(op) => !matched.has(`${op.method} ${op.path}`),
	);
	if (unmatched.length > 0) {
		console.log(`\n  WARNING: ${unmatched.length} unmatched operations`);
	}

	console.log("\n\nPhase 4: Convert TypeScript types to OpenAPI schemas");
	mergeIntoSpec(spec, operations, matched);
	console.log(`  Generated ${schemas.size} schemas`);

	console.log("\n\nPhase 4b: Post-process schemas for ogen compatibility");
	postProcessSchemas();

	// Re-write schemas to spec after post-processing
	for (const [name, schema] of schemas) {
		spec.components.schemas[name] = schema;
	}

	console.log("\n\nPhase 5: Write enhanced OpenAPI spec");
	// Deep clone to break shared references (prevents YAML alias explosion)
	const cleanSpec = JSON.parse(JSON.stringify(spec));
	const output = stringify(cleanSpec, {
		lineWidth: 120,
		defaultKeyType: "PLAIN",
		defaultStringType: "PLAIN",
	});
	writeFileSync(join(DIR, "openapi.yaml"), output, "utf-8");
	console.log(
		`  Written to openapi.yaml (${(output.length / 1024).toFixed(0)} KB)`,
	);

	console.log("\nDone.");
}

main();
