package bloblang

// Function defines a Bloblang function, arguments are provided to the
// constructor, allowing the implementation of this function to resolve them
// statically when possible.
type Function func() (any, error)

// FunctionConstructor defines a constructor for a Bloblang function, where a
// variadic list of arguments are provided.
//
// When a function is parsed from a mapping with static arguments the
// constructor will be called only once at parse time. When a function is parsed
// with dynamic arguments, such as a value derived from the mapping input, the
// constructor will be called on each invocation of the mapping with the derived
// arguments.
//
// For a convenient way to perform type checking and coercion on the arguments
// use an ArgSpec.
type FunctionConstructor func(args ...any) (Function, error)

// FunctionConstructorV2 defines a constructor for a Bloblang function where
// parameters are parsed using a ParamsSpec provided when registering the
// function.
//
// When a function is parsed from a mapping with static arguments the
// constructor will be called only once at parse time. When a function is parsed
// with dynamic arguments, such as a value derived from the mapping input, the
// constructor will be called on each invocation of the mapping with the derived
// arguments.
type FunctionConstructorV2 func(args *ParsedParams) (Function, error)

//------------------------------------------------------------------------------

// AdvancedFunction defines a Bloblang function that accesses the execution
// context of the mapping during invocation.
type AdvancedFunction func(ctx *ExecContext) (any, error)

// AdvancedFunctionConstructor defines a constructor for a Bloblang function
// where parameters are parsed using a ParamsSpec provided when registering the
// function, and the constructed function is provided an ExecContext.
type AdvancedFunctionConstructor func(args *ParsedParams) (AdvancedFunction, error)
