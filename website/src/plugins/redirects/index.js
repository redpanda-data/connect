const {docuHash} = require('@docusaurus/utils');

const redirects = [
  ["/docs/", "/docs/about", false],
  ["/buffers", "/docs/components/buffers/", true],
  ["/caches", "/docs/components/caches/", true],
  ["/conditions", "/docs/components/conditions/", true],
  ["/inputs", "/docs/components/inputs/", true],
  ["/logger", "/docs/components/logger/about", false],
  ["/metrics", "/docs/components/metrics/", true],
  ["/outputs", "/docs/components/outputs/", true],
  ["/processors", "/docs/components/processors/", true],
  ["/rate_limits", "/docs/components/rate_limits/", true],
  ["/tracers", "/docs/components/tracers/", true],
  ["/examples/kafka-delayed-retry", "/cookbooks/joining-streams#adding-a-retry-queue", false],
  ["/getting_started", "/docs/guides/getting_started", false],
  ["/serverless", "/docs/guides/serverless/about", false],
  ["/configuration", "/docs/configuration/about", false],
  ["/configuration_testing", "/docs/configuration/unit_testing", false],
  ["/config_interpolation", "/docs/configuration/interpolation", false],
  ["/pipeline", "/docs/configuration/processing_pipelines", false],
  ["/batching", "/docs/configuration/batching", false],
  ["/error_handling", "/docs/configuration/error_handling", false],
  ["/performance_tuning", "/docs/guides/performance_tuning", false],
  ["/monitoring", "/docs/guides/monitoring", false],
  ["/sync_responses", "/docs/guides/sync_responses", false],
  ["/workflows", "/docs/configuration/workflows", false],
  ["/streams", "/docs/guides/streams_mode/about", false],
  ["/docs/configuration/workflows", "/docs/components/processors/workflow", false],
  ["/docs/guides/aws", "/docs/guides/cloud/aws", false],
  ["/docs/guides/gcp", "/docs/guides/cloud/gcp", false],
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
