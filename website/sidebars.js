const {listPaths} = require('./src/plugins/components');

let inputs_docs = listPaths("inputs");
let processors_docs = listPaths("processors");
let outputs_docs = listPaths("outputs");
let caches_docs = listPaths("caches");
let rate_limits_docs = listPaths("rate_limits");
let buffers_docs = listPaths("buffers");
let metrics_docs = listPaths("metrics");
let tracers_docs = listPaths("tracers");

module.exports = {
  docs: [
    {
      type: 'doc',
      id: 'about',
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/about',
        'configuration/resources',
        'configuration/batching',
        'configuration/windowed_processing',
        'configuration/metadata',
        'configuration/error_handling',
        'configuration/interpolation',
        'configuration/secrets',
        'configuration/field_paths',
        'configuration/processing_pipelines',
        'configuration/unit_testing',
        'configuration/templating',
        'configuration/dynamic_inputs_and_outputs',
        'configuration/using_cue',
      ],
    },
    {
      type: 'category',
      label: 'Components',
      items: [
        'components/about',
        'components/http/about',
        {
          type: 'category',
          label: 'Inputs',
          items: inputs_docs,
        },
        {
          type: 'category',
          label: 'Processors',
          items: processors_docs,
        },
        {
          type: 'category',
          label: 'Outputs',
          items: outputs_docs,
        },
        {
          type: 'category',
          label: 'Caches',
          items: caches_docs,
        },
        {
          type: 'category',
          label: 'Rate Limits',
          items: rate_limits_docs,
        },
        {
          type: 'category',
          label: 'Buffers',
          items: buffers_docs,
        },
        {
          type: 'category',
          label: 'Metrics',
          items: metrics_docs,
        },
        {
          type: 'category',
          label: 'Tracers',
          items: tracers_docs,
        },
        'components/logger/about'
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/getting_started',
        {
          type: 'category',
          label: 'Bloblang',
          items: [
            'guides/bloblang/about',
            'guides/bloblang/walkthrough',
            'guides/bloblang/functions',
            'guides/bloblang/methods',
            'guides/bloblang/arithmetic',
            'guides/bloblang/advanced',
          ],
        },
        'guides/monitoring',
        'guides/performance_tuning',
        'guides/sync_responses',
        {
          type: 'category',
          label: 'Cloud Credentials',
          items: [
            'guides/cloud/aws',
            'guides/cloud/gcp',
          ],
        },
        {
          type: 'category',
          label: 'Serverless',
          items: [
            'guides/serverless/about',
            'guides/serverless/lambda',
          ],
        },
        {
          type: 'category',
          label: 'Streams Mode',
          items: [
            'guides/streams_mode/about',
            'guides/streams_mode/using_config_files',
            'guides/streams_mode/using_rest_api',
            'guides/streams_mode/streams_api',
          ],
        },
        {
          type: 'category',
          label: 'Migration',
          items: [
            'guides/migration/v4',
            'guides/migration/v3',
            'guides/migration/v2',
          ]
        }
      ],
    },
  ],
};
