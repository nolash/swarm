package file

import (
	"testing"

	"github.com/ethersphere/swarm/bmt"
)

func TestTreeParams(t *testing.T) {

	params := newTreeParams(sectionSize, branches)

	if params.section != 32 {
		t.Fatalf("section: expected %d, got %d", sectionSize, params.section)
	}

	if params.branches != 128 {
		t.Fatalf("branches: expected %d, got %d", branches, params.section)
	}

	if params.spans[2] != branches*branches {
		t.Fatalf("span %d: expected %d, got %d", 2, branches*branches, params.spans[1])
	}

}

func TestNewJob(t *testing.T) {

	var tgt *target
	var params *treeParams
	var writer bmt.SectionWriter

	params = newTreeParams(sectionSize, branches)

	job := newJob(params, tgt, writer, 1, branches)

	if job.level != 1 {
		t.Fatalf("job level expected 1, got %d", job.level)
	}

}
