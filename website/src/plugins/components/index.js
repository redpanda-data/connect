const path = require('path');
const fs = require('fs');
const {parseMarkdownString} = require('@docusaurus/utils');

function components(type) {
  return all_components(type).filter(c => c.status != "deprecated")
}

function all_components(type) {
  let components = [];
  let dir = path.join(__dirname, `../../../docs/components/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if ( !/about\.mdx?/.test(file) ) {
      let name = file.split('.').slice(0, -1).join('.');
      let data = fs.readFileSync(path.join(dir, file));
      const {frontMatter} = parseMarkdownString(data);
      frontMatter["name"] = name;
      components.push(frontMatter);
    }
  });
  return components;
}

function listPaths(type) {
  let paths = [`components/${type}/about`];

  let components = all_components(type);

  components
    .filter(c => c.status != "deprecated")
    .forEach(function (info) {
      paths.push(`components/${type}/${info.name}`);
    });

  let deprecatedPaths = components
    .filter(c => c.status == "deprecated")
    .map(c => `components/${type}/${c.name}`);

  if ( deprecatedPaths.length > 0 ) {
    paths.push({
      type: 'category',
      label: 'Deprecated',
      items: deprecatedPaths,
    });
  }

  return paths;
}

module.exports = {
  components: components,
  listPaths: listPaths,
};
