const path = require('path');
const {components} = require('./src/plugins/components');

module.exports = {
  title: 'Benthos',
  tagline: 'Fancy stream processing made operationally mundane',
  url: 'https://www.benthos.dev',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'benthosdev',
  projectName: 'benthos',
  customFields: {
    components: {
      inputs: components("inputs"),
      processors: components("processors"),
      outputs: components("outputs"),
      caches: components("caches"),
      rate_limits: components("rate_limits"),
      buffers: components("buffers"),
      metrics: components("metrics"),
      tracers: components("tracers"),
    },
  },
  themeConfig: {
    prism: {
      theme: require('./src/plugins/prism_themes/github'),
      darkTheme: require('./src/plugins/prism_themes/monokai'),
    },
    colorMode: {
      defaultMode: 'light',
    },
    image: 'img/og_img.png',
    metadata: [
      {name: 'keywords', content: 'benthos, stream processor, data engineering, ETL, ELT, event processor, go, golang'},
      {name: 'twitter:card', content: 'summary'},
    ],
    navbar: {
      title: 'Benthos',
      logo: {
        alt: 'Benthos Blobfish',
        src: 'img/logo.svg',
      },
      items: [
        {to: 'docs/about', label: 'Docs', position: 'left'},
        {to: 'cookbooks', label: 'Cookbooks', position: 'left'},
        {to: 'https://studio.benthos.dev', label: 'Studio', position: 'left'},
        {to: 'blog', label: 'Blog', position: 'left'},
        {to: 'community', label: 'Community', position: 'right'},
        {to: 'support', label: 'Support', position: 'right'},
        {
          href: 'https://github.com/benthosdev/benthos/releases/latest',
          position: 'right',
          className: 'header-download-link header-icon-link',
          'aria-label': 'Download',
        },
        {
          href: 'https://github.com/benthosdev/benthos',
          position: 'right',
          className: 'header-github-link header-icon-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Help',
          items: [
            {
              label: 'Support',
              to: 'support',
            },
            {
              label: 'Documentation',
              to: 'docs/guides/getting_started',
            },
            {
              label: 'Videos',
              to: 'videos',
            },
          ],
        },
        {
          title: 'Swag',
          items: [
            {
              label: 'Meet the Mascot',
              to: 'blobfish',
            },
            {
              label: 'Purchase Stickers',
              href: 'https://www.redbubble.com/people/earzola/shop',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Join the chat',
              to: 'community',
            },
            {
              label: 'See the Code',
              href: 'https://github.com/benthosdev/benthos',
            },
            {
              label: 'Sponsor the Developers',
              href: 'https://github.com/sponsors/Jeffail',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Ashley Jeffs.`,
    },
    announcementBar: {
      id: 'star_the_dang_repo',
      content: `<strong>Hey, 🫵 you, make sure you've <a target="_blank" rel="noopener noreferrer" href="https://github.com/benthosdev/benthos">⭐ starred the repo ⭐</a> otherwise you won't be entered into our daily prize draw for silent admiration.</strong>`,
      backgroundColor: 'var(--ifm-color-primary)',
      textColor: 'var(--ifm-background-color)',
      isCloseable: true,
    },
    algolia: {
      appId: 'WBY9Z65YR4',
      apiKey: 'a6c476911e6ecef76049a55d9798a51b',
      indexName: 'benthos',
      contextualSearch: true
    }
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/benthosdev/benthos/edit/main/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        blog: {
          feedOptions: {
            type: 'all',
          },
        },
      },
    ],
  ],
  plugins: [
    path.resolve(__dirname, './src/plugins/analytics'),
    [
      require.resolve("./src/plugins/cookbooks/compiled/index"),
      {
        path: 'cookbooks',
        routeBasePath: 'cookbooks',
        include: ['*.md', '*.mdx'],
        exclude: [],
        guideListComponent: '@theme/CookbookListPage',
        guidePostComponent: '@theme/CookbookPage',
      },
    ],
  ],
};

