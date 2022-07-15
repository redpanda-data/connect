/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {linkify} from './cookbookUtils';
import type {CookbookMarkdownLoaderOptions} from './types';
import type {LoaderContext} from 'webpack';

export default function markdownLoader(
  this: LoaderContext<CookbookMarkdownLoaderOptions>,
  source: string,
): void {
  const filePath = this.resourcePath;
  const fileString = source;
  const callback = this.async();
  const markdownLoaderOptions = this.getOptions();

  // Linkify blog posts
  let finalContent = linkify({
    fileString,
    filePath,
    ...markdownLoaderOptions,
  });

  return callback(null, finalContent);
}
