/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import type {BrokenMarkdownLink, ContentPaths} from '@docusaurus/utils';
import type {MDXOptions} from '@docusaurus/mdx-loader';

export type CookbookContentPaths = ContentPaths;

export type CookbookBrokenMarkdownLink = BrokenMarkdownLink<CookbookContentPaths>;
export type CookbookMarkdownLoaderOptions = {
  siteDir: string;
  contentPaths: CookbookContentPaths;
  sourceToPermalink: {[aliasedPath: string]: string};
  onBrokenMarkdownLink: (brokenMarkdownLink: CookbookBrokenMarkdownLink) => void;
};

export type CookbookPostMetadata = {
  readonly source: string;
  readonly title: string;
  readonly permalink: string;
  readonly description: string;
};

export type CookbookPost = {
  id: string;
  metadata: CookbookPostMetadata;
  content: string;
};

export type PluginOptions = MDXOptions & {
  id?: string;
  path: string;
  routeBasePath: string;
  include: string[];
  exclude: string[];
  guideListComponent: string;
  guidePostComponent: string;
};

export type CookbookContent = {
  cookbookPosts: CookbookPost[];
};

export type CookbookPostFrontMatter = {
  id?: string;
  title?: string;
  description?: string;
  slug?: string;
  draft?: boolean;
  keywords?: string[];
};
