package sync

func ToSet(str []string) map[string]bool {
	set := make(map[string]bool)
	for _, s := range str {
		set[s] = true
	}
	return set
}

func Difference(first, second map[string]bool) map[string]bool {
	set := make(map[string]bool)

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
