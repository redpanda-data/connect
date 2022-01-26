const path = require('path');

const isProd = process.env.NODE_ENV === 'production';

module.exports = function(context, options) {
  return {
    name: 'blob-analytics',
    getClientModules() {
      return isProd ? [path.resolve(__dirname, './client')] : [];
    },
  };
};
