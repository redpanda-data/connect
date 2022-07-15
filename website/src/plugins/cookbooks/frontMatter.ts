/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {
  JoiFrontMatter as Joi, // Custom instance for front matter
  validateFrontMatter,
} from '@docusaurus/utils-validation';
import type {CookbookPostFrontMatter} from './types';

const CookbookFrontMatterSchema = Joi.object<CookbookPostFrontMatter>({
  id: Joi.string(),
  title: Joi.string().allow(''),
  description: Joi.string().allow(''),
  draft: Joi.boolean(),
  slug: Joi.string(),
  keywords: Joi.array().items(Joi.string().required()),
}).messages({
  'deprecate.error':
  '{#label} blog frontMatter field is deprecated. Please use {#alternative} instead.',
});

export function validateCookbookPostFrontMatter(frontMatter: {
  [key: string]: unknown;
}): CookbookPostFrontMatter {
  return validateFrontMatter(frontMatter, CookbookFrontMatterSchema);
}
