package domain

import "testing"

func TestFanInDAG(t *testing.T) {
	w := WorkflowSpec{ID: "test", Version: "v1", Steps: []StepSpec{{Name: "a", Function: FunctionRef{"f", "v1"}}, {Name: "b", Function: FunctionRef{"f", "v1"}, DependsOn: []string{"a", "a"}}, {Name: "c", Function: FunctionRef{"f", "v1"}, DependsOn: []string{"a"}}, {Name: "d", Function: FunctionRef{"f", "v1"}, Input: InputPlan{Fields: map[string]InputSource{"left": {Kind: "step_output", Step: "b"}, "right": {Kind: "step_output", Step: "c"}}}}}}
	if err := NormalizeWorkflow(&w); err != nil {
		t.Fatal(err)
	}
	if w.OutputStep != "d" || len(w.Steps[1].DependsOn) != 1 || len(w.Steps[3].DependsOn) != 2 {
		t.Fatalf("unexpected normalized DAG: %+v", w)
	}
	w.Steps[0].DependsOn = []string{"d"}
	if err := NormalizeWorkflow(&w); err == nil {
		t.Fatal("accepted cycle")
	}
}

func TestPayloadUnionAndPointer(t *testing.T) {
	if err := ValidatePayload(PayloadRef{Inline: []byte(`null`)}); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePayload(PayloadRef{}); err == nil {
		t.Fatal("accepted empty payload")
	}
	if err := ValidatePayload(PayloadRef{Inline: []byte(`{}`), Object: &ObjectRef{ID: "x"}}); err == nil {
		t.Fatal("accepted mixed payload")
	}
	for _, p := range []string{"", "/a~1b/~0"} {
		if err := ValidatePointer(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidatePointer("/a~2"); err == nil {
		t.Fatal("accepted invalid escape")
	}
}
