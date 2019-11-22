package file

import "testing"

func TestDataSizeToSection(t *testing.T) {

	sizes := []int{chunkSize - 1, chunkSize, chunkSize + 1}
	expects := []int{branches, branches, branches + 1}

	for j, size := range sizes {
		r := dataSizeToSection(size, sectionSize)
		expect := expects[j]
		if expect != r {
			t.Fatalf("size %d section %d: expected %d, got %d", size, sectionSize, expect, r)
		}
	}

}

//func TestDataSizeToLevelSection(t *testing.T) {
//
//	sizes := []int{4097}
//	levels := []int{1}
//	expects := []int{1}
//
//	for i, lvl := range levels {
//		for j, size := range sizes {
//			r := dataSizeToLevelSection(lvl, size, sectionSize, branches)
//			k := i*len(sizes) + j
//			expect := expects[k]
//			if expect != r {
//				t.Fatalf("size %d level %s: expected %d, got %d", size, lvl, expect, r)
//			}
//		}
//	}
//
//}
