const path = require('path');
const {components} = require('./src/plugins/components');

module.exports = {
  title: 'Benthos',
  tagline: 'Fancy stream processing made operationally mundane',
  url: 'https://www.benthos.dev',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'Jeffail',
  projectName: 'benthos',
  customFields: {
    components: {
      inputs: components("inputs"),
      processors: components("processors"),
      conditions: components("conditions"),
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
    metadata: [{name: 'twitter:card', content: 'summary'}],
    navbar: {
      title: 'Benthos',
      logo: {
        alt: 'Benthos Blobfish',
        src: 'img/logo.svg',
      },
      items: [
        {to: 'docs/about', label: 'Docs', position: 'left'},
        {to: 'cookbooks', label: 'Cookbooks', position: 'left'},
        {to: 'blog', label: 'Blog', position: 'left'},
        {to: 'videos', label: 'Videos', position: 'left'},
        {to: 'community', label: 'Community', position: 'right'},
        {
          href: 'https://github.com/Jeffail/benthos/releases/latest',
          position: 'right',
          className: 'header-download-link header-icon-link',
          'aria-label': 'Download',
        },
        {
          href: 'https://github.com/Jeffail/benthos',
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
          title: 'Docs',
          items: [
            {
              label: 'Getting started',
              to: 'docs/guides/getting_started',
            },
            {
              label: 'Videos',
              to: 'videos',
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
              label: 'GitHub',
              href: 'https://github.com/Jeffail/benthos',
            },
          ],
        },
        {
          title: 'Social',
          items: [
            {
              label: 'Get in touch',
              to: 'community',
            },
            {
              label: 'Mascot',
              to: 'blobfish',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Ashley Jeffs.`,
    },
    algolia: {
      appId: 'WBY9Z65YR4',
      apiKey: 'a6c476911e6ecef76049a55d9798a51b',
      indexName: 'benthos',
      contextualSearch: false
    }
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/Jeffail/benthos/edit/master/website/',
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
    path.resolve(__dirname, './src/plugins/cookbooks'),
    path.resolve(__dirname, './src/plugins/redirects'),
  ],
};

