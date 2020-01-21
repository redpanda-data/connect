const path = require('path');
const fs = require('fs');

function listNames(type) {
  let names = [];
  let dir = path.join(__dirname, `../../../docs/components/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if ( !/about\.mdx?/.test(file) ) {
      names.push(file.split('.').slice(0, -1).join('.'));
    }
  });
  return names;
}

function listPaths(type) {
  let paths = [`components/${type}/about`];

  listNames(type).forEach(function (name) {
    paths.push(`components/${type}/${name}`);
  });

  return paths;
}

module.exports = {
  listNames: listNames,
  listPaths: listPaths,
};