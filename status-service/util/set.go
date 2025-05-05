package util

func NewSet() map[string]bool {
	return make(map[string]bool)
}

func ToSet(str []string) map[string]bool {
	set := NewSet()
	for _, s := range str {
		set[s] = true
	}
	return set
}

func AddToSet(set map[string]bool, values ...string) {
	for _, key := range values {
		set[key] = true
	}
}

func Difference(first, second map[string]bool) map[string]bool {
	set := NewSet()

	for k := range first {
		if _, ok := second[k]; !ok { // we could also do ok := second[k]
			set[k] = true
		}
	}

	for k := range second {
		if _, ok := first[k]; !ok { // we could also do ok := second[k]
			set[k] = true
		}
	}

	return set
}
