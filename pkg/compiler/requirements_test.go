package compiler

import (
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

func TestGetRequirements(t *testing.T) {
	g := &core.WorkflowTemplate{
		Nodes: []*core.Node{
			{
				Target: &core.Node_TaskNode{
					TaskNode: &core.TaskNode{
						Reference: &core.TaskNode_ReferenceId{
							ReferenceId: &core.Identifier{Name: "Task_1"},
						},
					},
				},
			},
			{
				Target: &core.Node_TaskNode{
					TaskNode: &core.TaskNode{
						Reference: &core.TaskNode_ReferenceId{
							ReferenceId: &core.Identifier{Name: "Task_2"},
						},
					},
				},
			},
			{
				Target: &core.Node_WorkflowNode{
					WorkflowNode: &core.WorkflowNode{
						Reference: &core.WorkflowNode_LaunchplanRef{
							LaunchplanRef: &core.Identifier{Name: "Graph_1"},
						},
					},
				},
			},
			{
				Target: &core.Node_BranchNode{
					BranchNode: &core.BranchNode{
						IfElse: &core.IfElseBlock{
							Case: &core.IfBlock{
								ThenNode: &core.Node{
									Target: &core.Node_WorkflowNode{
										WorkflowNode: &core.WorkflowNode{
											Reference: &core.WorkflowNode_LaunchplanRef{
												LaunchplanRef: &core.Identifier{Name: "Graph_1"},
											},
										},
									},
								},
							},
							Other: []*core.IfBlock{
								{
									ThenNode: &core.Node{
										Target: &core.Node_TaskNode{
											TaskNode: &core.TaskNode{
												Reference: &core.TaskNode_ReferenceId{
													ReferenceId: &core.Identifier{Name: "Task_3"},
												},
											},
										},
									},
								},
								{
									ThenNode: &core.Node{
										Target: &core.Node_BranchNode{
											BranchNode: &core.BranchNode{
												IfElse: &core.IfElseBlock{
													Case: &core.IfBlock{
														ThenNode: &core.Node{
															Target: &core.Node_WorkflowNode{
																WorkflowNode: &core.WorkflowNode{
																	Reference: &core.WorkflowNode_LaunchplanRef{
																		LaunchplanRef: &core.Identifier{Name: "Graph_2"},
																	},
																},
															},
														},
													},
													Other: []*core.IfBlock{
														{
															ThenNode: &core.Node{
																Target: &core.Node_TaskNode{
																	TaskNode: &core.TaskNode{
																		Reference: &core.TaskNode_ReferenceId{
																			ReferenceId: &core.Identifier{Name: "Task_4"},
																		},
																	},
																},
															},
														},
														{
															ThenNode: &core.Node{
																Target: &core.Node_TaskNode{
																	TaskNode: &core.TaskNode{
																		Reference: &core.TaskNode_ReferenceId{
																			ReferenceId: &core.Identifier{Name: "Task_5"},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	subWorkflows := make([]*core.WorkflowTemplate, 0)
	reqs, err := GetRequirements(g, subWorkflows)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(reqs.GetRequiredTaskIds()))
	assert.Equal(t, 2, len(reqs.GetRequiredLaunchPlanIds()))
}

func TestGetRequirements_Cyclic(t *testing.T) {
	t.Run("Simple Cycle", func(t *testing.T) {
		wf1 := &core.WorkflowTemplate{
			Id: &core.Identifier{Name: "repo"},
			Interface: &core.TypedInterface{
				Inputs: createVariableMap(map[string]*core.Variable{
					"x": {
						Type: getIntegerLiteralType(),
					},
				}),
				Outputs: createVariableMap(map[string]*core.Variable{
					"x": {
						Type: getIntegerLiteralType(),
					},
				}),
			},
			Nodes: []*core.Node{
				{
					Id: "subwf",
					Target: &core.Node_WorkflowNode{
						WorkflowNode: &core.WorkflowNode{
							Reference: &core.WorkflowNode_SubWorkflowRef{
								SubWorkflowRef: &core.Identifier{Name: "repo"},
							},
						},
					},
				},
			},
			Outputs: []*core.Binding{newVarBinding("node_456", "x", "x")},
		}

		_, err := GetRequirements(wf1, []*core.WorkflowTemplate{wf1})
		assert.Error(t, err)
		t.Log(err)
	})

	t.Run("Deep Cycle", func(t *testing.T) {
		createWF := func(name, dependsOn string) *core.WorkflowTemplate {
			return &core.WorkflowTemplate{
				Id: &core.Identifier{Name: name},
				Interface: &core.TypedInterface{
					Inputs: createVariableMap(map[string]*core.Variable{
						"x": {
							Type: getIntegerLiteralType(),
						},
					}),
					Outputs: createVariableMap(map[string]*core.Variable{
						"x": {
							Type: getIntegerLiteralType(),
						},
					}),
				},
				Nodes: []*core.Node{
					{
						Id: "subwf",
						Target: &core.Node_WorkflowNode{
							WorkflowNode: &core.WorkflowNode{
								Reference: &core.WorkflowNode_SubWorkflowRef{
									SubWorkflowRef: &core.Identifier{Name: dependsOn},
								},
							},
						},
					},
				},
				Outputs: []*core.Binding{newVarBinding("node_456", "x", "x")},
			}
		}
		wf1 := createWF("wf1", "wf2")
		wf2 := createWF("wf2", "wf3")
		wf3 := createWF("wf3", "wf1")

		_, err := GetRequirements(wf1, []*core.WorkflowTemplate{wf2, wf3, wf1})
		assert.Error(t, err)
		t.Log(err)
	})
}
