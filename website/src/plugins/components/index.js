const path = require('path');
const fs = require('fs');
const {parseMarkdownString} = require('@docusaurus/utils');

function components(type) {
  let components = [];
  let dir = path.join(__dirname, `../../../docs/components/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if ( !/about\.mdx?/.test(file) ) {
      let name = file.split('.').slice(0, -1).join('.');
      let data = fs.readFileSync(path.join(dir, file));
      const {frontMatter} = parseMarkdownString(data);
      if (frontMatter.deprecated === true) {
        return;
      }
      frontMatter["name"] = name;
      components.push(frontMatter);
    }
  });
  return components;
}

function deprecatedComponents(type) {
  let components = [];
  let dir = path.join(__dirname, `../../../docs/components/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    let name = file.split('.').slice(0, -1).join('.');
    let data = fs.readFileSync(path.join(dir, file));
    const {frontMatter} = parseMarkdownString(data);
    if (frontMatter.deprecated === true) {
      frontMatter["name"] = name;
      components.push(frontMatter);
    }
  });
  return components;
}

function listPaths(type) {
  let paths = [`components/${type}/about`];

  components(type).forEach(function (info) {
    paths.push(`components/${type}/${info.name}`);
  });

  return paths;
}

function listDeprecatedPaths(type) {
  let paths = [];

  deprecatedComponents(type).forEach(function (info) {
    paths.push(`components/${type}/${info.name}`);
  });

  return paths;
}

module.exports = {
  listPaths: listPaths,
  listDeprecatedPaths: listDeprecatedPaths,
  components: components,
  deprecatedComponents: deprecatedComponents,
};