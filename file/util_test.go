package file

import "testing"

func TestDataSizeToSectionCount(t *testing.T) {

	sizes := []int{4095, 4096, 4097}
	sections := []int{32, 128}
	expects := []int{128, 128, 129, 32, 32, 33}

	for i, section := range sections {
		for j, size := range sizes {
			r := dataSizeToSectionCount(size, section)
			ei := i*len(sizes) + j
			expect := expects[ei]
			if expect != r {
				t.Fatalf("size %d section %d expect %d, got %d", size, section, expect, r)
			}
		}
	}

}
