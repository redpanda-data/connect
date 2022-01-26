const host = window.location.hostname;

function poke(path) {
  // TODO: Allow this to be configured
  fetch(`https://poke.benthos.dev/poke?h=${encodeURIComponent(host)}&p=${encodeURIComponent(path)}`, { method: "POST" })
    .catch((error) => console.error(error))
}

module.exports = (() => {
  poke(window.location.pathname);
  return {
    onRouteUpdate({location}) {
      poke(location.pathname);
    },
  };
})();
