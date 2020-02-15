const {docuHash} = require('@docusaurus/utils');

const redirects = [
  ["/docs/", "/docs/about/", false],
  ["/docs/buffers", "/docs/components/buffers/", true],
  ["/docs/caches", "/docs/components/caches/", true],
  ["/docs/conditions", "/docs/components/conditions/", true],
  ["/docs/inputs", "/docs/components/inputs/", true],
  ["/docs/logger", "/docs/components/logger/about", false],
  ["/docs/metrics", "/docs/components/metrics/", true],
  ["/docs/outputs", "/docs/components/outputs/", true],
  ["/docs/processors", "/docs/components/processors/", true],
  ["/docs/rate_limits", "/docs/components/rate_limits/", true],
  ["/docs/tracers", "/docs/components/tracers/", true],
  ["/docs/examples/kafka-delayed-retry", "/cookbooks/joining-streams/#adding-a-retry-queue", false],
  ["/docs/cookbooks/", "/cookbooks/", false],
  ["/docs/getting_started", "/docs/guides/getting_started", false],
  ["/docs/serverless", "/docs/guides/serverless/about", false],
  ["/docs/configuration", "/docs/configuration/about", false],
  ["/docs/configuration_testing", "/docs/configuration/unit_testing", false],
  ["/docs/config_interpolation", "/docs/configuration/interpolation", false],
  ["/docs/pipeline", "/docs/configuration/processing_pipelines", false],
  ["/docs/batching", "/docs/configuration/batching", false],
  ["/docs/error_handling", "/docs/configuration/error_handling", false],
  ["/docs/performance_tuning", "/docs/guides/performance_tuning", false],
  ["/docs/monitoring", "/docs/guides/monitoring", false],
  ["/docs/sync_responses", "/docs/guides/sync_responses", false],
  ["/docs/workflows", "/docs/configuration/workflows", false],
  ["/docs/streams", "/docs/guides/streams_mode/about", false],
];

module.exports = function() {
  return {
    name: 'docusaurus-plugin',
    async contentLoaded({actions}) {
      const {addRoute, createData} = actions;

      await Promise.all(
        redirects.map(async redirect => {
          const dest = JSON.stringify({to: redirect[1], forComponent: redirect[2]});
          const destPath = await createData(`${docuHash(redirect[0])}.json`, dest);

          addRoute({
            path: redirect[0],
            component: '@site/src/exports/redirect.js',
            modules: {
              dest: destPath,
            },
            exact: true,
          });
        }),
      );
    },
  };
};