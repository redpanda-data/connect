function poke(host, path) {
  // TODO: Allow this to be configured
  fetch(`https://poke.benthos.dev/poke?h=${encodeURIComponent(host)}&p=${encodeURIComponent(path)}`, { method: "POST" })
    .catch((error) => console.error(error))
}

module.exports = (() => {
  if (typeof window !== "object") {
    return {};
  }

  const host = window.location.hostname;

  poke(host, window.location.pathname);
  return {
    onRouteUpdate({location}) {
      poke(host, location.pathname);
    },
  };
})();
