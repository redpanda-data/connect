const path = require('path');
const fs = require('fs');

const skipList = {
  "processors": {
    "json": true,
    "encode": true,
    "decode": true,
    "hash": true,
    // "filter": true,
    // "filter_parts": true,
    "merge_json": true,
    "metadata": true,
    "number": true,
    "text": true,
  },
  "conditions": {
    "json": true,
    "metadata": true,
    "number": true,
    "text": true,
  }
};

function listNames(type) {
  let names = [];
  let skip = skipList[type];
  let dir = path.join(__dirname, `../../../docs/components/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if ( !/about\.mdx?/.test(file) ) {
      let name = file.split('.').slice(0, -1).join('.');
      if (skip !== undefined && skip[name] !== undefined) {
        return;
      }
      names.push(name);
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