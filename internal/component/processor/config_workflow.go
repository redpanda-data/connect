package processor

// WorkflowConfig is a config struct containing fields for the Workflow
// processor.
type WorkflowConfig struct {
	MetaPath        string                  `json:"meta_path" yaml:"meta_path"`
	Order           [][]string              `json:"order" yaml:"order"`
	BranchResources []string                `json:"branch_resources" yaml:"branch_resources"`
	Branches        map[string]BranchConfig `json:"branches" yaml:"branches"`
}

// NewWorkflowConfig returns a default WorkflowConfig.
func NewWorkflowConfig() WorkflowConfig {
	return WorkflowConfig{
		MetaPath:        "meta.workflow",
		Order:           [][]string{},
		BranchResources: []string{},
		Branches:        map[string]BranchConfig{},
	}
}
