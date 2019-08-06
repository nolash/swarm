package network

// CapabilitiesAPI abstracts RPC API access to capabilities controls
// will in the future provide notifications of capability changes
type CapabilitiesAPI struct {
	*Capabilities
}

// NetCapabilitiesAPI creates new API abstraction around provided Capabilities object
func NewCapabilitiesAPI(c *Capabilities) *CapabilitiesAPI {
	return &CapabilitiesAPI{
		Capabilities: c,
	}
}

// Set sets the appropriate flags for the corresponding Capability in the Capabilities array
// If the Capability does not yet exist in the Capabilities array it is first added
func (a *CapabilitiesAPI) Register(cp *Capability) error {
	return a.add(cp)
}

func (a *CapabilitiesAPI) IsRegistered(id CapabilityId) bool {
	return a.get(id) != nil
}

func (a *CapabilitiesAPI) IsSet(id CapabilityId, idx int) bool {
	c := a.get(id)
	if c == nil {
		return false
	} else if idx > len(c.Cap)-1 {
		return false
	}
	return c.Cap[idx]
}
