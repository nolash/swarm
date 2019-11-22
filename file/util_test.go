package file

import "testing"

func TestDataSizeToSection(t *testing.T) {

	sizes := []int{chunkSize - 1, chunkSize, chunkSize + 1}
	expects := []int{branches - 1, branches - 1, branches}

	for j, size := range sizes {
		r := dataSizeToSectionIndex(size, sectionSize)
		expect := expects[j]
		if expect != r {
			t.Fatalf("size %d section %d: expected %d, got %d", size, sectionSize, expect, r)
		}
	}

}

func TestDataSizeToLevelSectionIndex(t *testing.T) {

	params := newTreeParams(sectionSize, branches)
	sizes := []int{chunkSize - 1, chunkSize, chunkSize + 1, chunkSize * 2, chunkSize*2 + 1, chunkSize*branches + 1}
	levels := []int{1, 2}
	expects := []int{0, 0, 1, 1, 2, 128, 0, 0, 0, 0, 0, 1}

	for i, lvl := range levels {
		for j, size := range sizes {
			r := dataSizeToLevelSectionIndex(params, lvl, size)
			k := i*len(sizes) + j
			expect := expects[k]
			if expect != r {
				t.Fatalf("size %d level %d: expected %d, got %d", size, lvl, expect, r)
			}
		}
	}

}
