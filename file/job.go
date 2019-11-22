package file

import "github.com/ethersphere/swarm/bmt"

type hasherParams struct {
	section  int
	branches int
}

type target struct {
	size     int
	sections int
	level    int
}

func newHasherParams(section int, branches int) *hasherParams {
	return &hasherParams{
		section:  section,
		branches: branches,
	}
}

type job struct {
	level int
	start int
}

func newJob(params *hasherParams, tgt *target, writer bmt.SectionWriter, level int, dataSection int) *job {
	return &job{
		level: level,
	}
}
