package file

import (
	"encoding/binary"
	"math"

	"github.com/ethersphere/swarm/log"
)

// creates a binary span size representation
// to pass to bmt.SectionWriter
// TODO: move to bmt.SectionWriter, which is the object actually using this
func lengthToSpan(length int) []byte {
	spanBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(spanBytes, uint64(length))
	return spanBytes
}

// calculates the section index of the given byte size
func dataSizeToSectionIndex(length int, sectionSize int) int {
	return (length - 1) / sectionSize
}

func dataSizeToSectionCount(length int, sectionSize int) int {
	return dataSizeToSectionIndex(length, sectionSize) + 1
}

func dataSectionToLevelSection(p *treeParams, lvl int, sections int) int {
	span := p.Spans[lvl]
	return sections / span
}

func dataSectionToLevelBoundary(p *treeParams, lvl int, section int) int {
	span := p.Spans[lvl+1]
	//levelSection := dataSectionToLevelSection(p, lvl, section)
	//log.Trace("levelboundary", "levelsection", levelSection, "section", section, "span", span)
	spans := section / span
	spanBytes := spans * span
	log.Trace("levelboundary", "spans", spans, "section", section, "span", span)
	return spanBytes
}

// calculate how many levels the tree will. includes root hash as level
func getLevelsFromLength(l int, sectionSize int, branches int) int {
	if l == 0 {
		return 0
	} else if l <= sectionSize*branches {
		return 1
	}
	c := (l - 1) / (sectionSize)

	return int(math.Log(float64(c))/math.Log(float64(branches)) + 1)
}

//// calculates amount of sections the given data affects
//func sectionCount(b []byte, sectionSize uint64) uint64 {
//	//return uint64(len(b)-1)/sectionSize + 1
//	return sectionCountNum(len(b), sectionSize)
//}
//
//// returns number of sections a slice of data comprises
//// rounded up to nearest sectionsize boundary
//func (m *FileSplitterTwo) sectionCount(b []byte) uint64 {
//	return sectionCount(b, uint64(m.SectionSize()))
//}
//
//// calculates section of the parent job in its level that will be written to
//func (m *FileSplitterTwo) getParentSection(idx uint64) uint64 {
//	return idx / m.branches
//}
//
//// calculates the lower branch boundary to the corresponding section
//func (m *FileSplitterTwo) getIndexFromSection(sectionCount uint64) uint64 {
//	return sectionCount / m.branches
//}
//
//// calculates the lower chunk boundary of data level with respect to the start of the span of the current position in the level
//// TODO: use table for span
//func (m *FileSplitterTwo) getLowerBoundaryByLevel(dataCount uint64, level int32) uint64 {
//	log.Warn("lowerboundary", "span", m.spanTable[level], "level", level, "dataCount", dataCount)
//	return uint64(int32(dataCount-1) / int32(m.spanTable[level]))
//}
