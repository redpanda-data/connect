/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import path from 'path';
import logger from '@docusaurus/logger';
import {
  normalizeUrl,
  docuHash,
  aliasedSitePath,
  addTrailingPathSeparator,
  getContentPathList,
  DEFAULT_PLUGIN_ID,
} from '@docusaurus/utils';
import {
  generateCookbookPosts,
  getSourceToPermalink,
} from './cookbookUtils';
import type {LoadContext, Plugin} from '@docusaurus/types';

import type {
  PluginOptions,
  CookbookContent,
  CookbookContentPaths,
  CookbookMarkdownLoaderOptions,
} from './types';

export default async function pluginContentCookbook(
  context: LoadContext,
  options: PluginOptions,
): Promise<Plugin<CookbookContent>> {
  const {
    siteDir,
    siteConfig,
    generatedFilesDir,
  } = context;
  const {onBrokenMarkdownLinks, baseUrl} = siteConfig;

  const contentPaths: CookbookContentPaths = {
    contentPath: path.resolve(siteDir, options.path),
    contentPathLocalized: path.resolve(siteDir, options.path),
  };
  const pluginId = options.id ?? DEFAULT_PLUGIN_ID;

  const pluginDataDirRoot = path.join(
    generatedFilesDir,
    'cookbooks',
  );
  const dataDir = path.join(pluginDataDirRoot, pluginId);

  return {
    name: 'cookbooks',

    getPathsToWatch() {
      const {include} = options;
      const contentMarkdownGlobs = getContentPathList(contentPaths).flatMap(
        (contentPath) => include.map((pattern) => `${contentPath}/${pattern}`),
      );

      return [...contentMarkdownGlobs].filter(
        Boolean,
      ) as string[];
    },

    // Fetches guide contents and returns metadata for the necessary routes.
    async loadContent() {
      const guidePosts = await generateCookbookPosts(contentPaths, context, options);
      return {
        cookbookPosts: guidePosts,
      };
    },

    async contentLoaded({content: cookbookContents, actions}) {
      const {
        routeBasePath,
        guideListComponent,
        guidePostComponent,
      } = options;

      const aliasedSource = (source: string) =>
        `~cookbooks/${path.relative(pluginDataDirRoot, source)}`;

      const {addRoute, createData} = actions;
      const {
        cookbookPosts,
      } = cookbookContents;

      // Create routes for guide entries.
      await Promise.all(
        cookbookPosts.map(async guidePost => {
          const {metadata} = guidePost;
          await createData(
            // Note that this created data path must be in sync with metadataPath provided to mdx-loader
            `${docuHash(metadata.source)}.json`,
            JSON.stringify(metadata, null, 2),
          );

          addRoute({
            path: metadata.permalink,
            component: guidePostComponent,
            exact: true,
            modules: {
              content: metadata.source,
            },
          });
        }),
      );

      const basePageUrl = normalizeUrl([baseUrl, routeBasePath]);

      const listPageMetadataPath = await createData(
        `${docuHash(`${basePageUrl}`)}.json`,
        JSON.stringify({}, null, 2),
      );

      let basePageItems = cookbookPosts.map(cookbookPost => {
        const {metadata} = cookbookPost;
        // To tell routes.js this is an import and not a nested object to recurse.
        return {
          content: {
            __import: true,
            path: metadata.source,
            query: {
              truncated: true,
            },
          },
        };
      });

      addRoute({
        path: basePageUrl,
        component: guideListComponent,
        exact: true,
        modules: {
          items: basePageItems,
          metadata: aliasedSource(listPageMetadataPath),
        },
      });
    },

    configureWebpack(_config, isServer, {getJSLoader}, content) {
      const {
        rehypePlugins,
        remarkPlugins,
      } = options;

      const markdownLoaderOptions: CookbookMarkdownLoaderOptions = {
        siteDir,
        contentPaths,
        sourceToPermalink: getSourceToPermalink(content.cookbookPosts),
        onBrokenMarkdownLink: (brokenMarkdownLink) => {
          if (onBrokenMarkdownLinks === 'ignore') {
            return;
          }
          logger.report(
            onBrokenMarkdownLinks,
          )`Blog markdown link couldn't be resolved: (url=${brokenMarkdownLink.link}) in path=${brokenMarkdownLink.filePath}`;
        },
      };

      const contentDirs = getContentPathList(contentPaths);
      return {
        resolve: {
          alias: {
            '~cookbooks': pluginDataDirRoot,
          },
        },
        module: {
          rules: [
            {
              test: /(\.mdx?)$/,
              include: contentDirs.map(addTrailingPathSeparator),
              use: [
                getJSLoader({isServer}),
                {
                  loader: require.resolve('@docusaurus/mdx-loader'),
                  options: {
                    remarkPlugins,
                    rehypePlugins,
                    staticDirs: siteConfig.staticDirectories.map((dir) =>
                      path.resolve(siteDir, dir),
                    ),
                    // Note that metadataPath must be the same/ in-sync as the path from createData for each MDX
                    metadataPath: (mdxPath: string) => {
                      // Note that metadataPath must be the same/in-sync as
                      // the path from createData for each MDX.
                      const aliasedPath = aliasedSitePath(mdxPath, siteDir);
                      return path.join(
                        dataDir,
                        `${docuHash(aliasedPath)}.json`,
                      );
                    },
                    markdownConfig: siteConfig.markdown,
                  },
                },
                {
                  loader: path.resolve(__dirname, './markdownLoader.js'),
                  options: markdownLoaderOptions,
                },
              ].filter(Boolean),
            },
          ],
        },
      };
    },
  };
}
