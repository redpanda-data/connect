const path = require('path');
const fs = require('fs');

function scan_component_docs(component) {
  let docs = [`components/${component}/about`];

  let dir = path.join(__dirname, `docs/components/${component}`);
  fs.readdirSync(dir).forEach(function (file) {
    if ( file != 'about.md' ) {
      let name = file.split('.').slice(0, -1).join('.');
      docs.push(`components/${component}/${name}`);
    }
  });

  return docs;
}

let inputs_docs = scan_component_docs("inputs");
let processors_docs = scan_component_docs("processors");
let conditions_docs = scan_component_docs("conditions");
let outputs_docs = scan_component_docs("outputs");
let caches_docs = scan_component_docs("caches");
let rate_limits_docs = scan_component_docs("rate_limits");
let buffers_docs = scan_component_docs("buffers");
let metrics_docs = scan_component_docs("metrics");
let tracers_docs = scan_component_docs("tracers");

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
        'configuration/batching',
        'configuration/error_handling',
        'configuration/interpolation',
        'configuration/field_paths',
        'configuration/processing_pipelines',
        'configuration/unit_testing',
        'configuration/workflows',
        'configuration/dynamic_inputs_and_outputs',
      ],
    },
    {
      type: 'category',
      label: 'Components',
      items: [
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
          label: 'Conditions',
          items: conditions_docs,
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
        'guides/monitoring',
        'guides/performance_tuning',
        'guides/sync_responses',
        'guides/aws',
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
            'guides/migration/v2',
            'guides/migration/v3',
          ]
        }
      ],
    },
  ],
};
