package lib
import "strings"

#Product: {
  name: string
  price: > 0.
  slug: strings.Replace(strings.ToLower(name), " ", "-", -1)
}
