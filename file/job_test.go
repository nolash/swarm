package file

import (
	"testing"

	"github.com/ethersphere/swarm/bmt"
)

func TestNewJob(t *testing.T) {

	var tgt *target
	var params *hasherParams
	var writer bmt.SectionWriter

	params = newHasherParams(32, 128)

	job := newJob(params, tgt, writer, 1, 128)

	if job.level != 1 {
		t.Fatalf("job level expected 1, got %d", job.level)
	}

}
