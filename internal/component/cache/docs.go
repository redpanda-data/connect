package cache

// Description appends standard feature descriptions to a cache description
// based on various features of the cache.
func Description(supportsPerKeyTTL bool, content string) string {
	if supportsPerKeyTTL {
		content = content + `

This cache type supports setting the TTL individually per key by using the
dynamic ` + "`ttl`" + ` field of a cache processor or output in order to
override the general TTL configured at the cache resource level.`
	}
	return content
}
