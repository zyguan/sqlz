package stmtflow

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zyguan/sqlz/resultset"
)

var resultData = []string{
	"H4sIAAAAAAAA/1SPz0v7QBDF5222X75pA+LBi2dPHjx7lVZIoRSp3koP2zjRwDaR7Ab1aK1af/3NI5uKtOxl5u2bx/scy1MEJUtCBN2vrIO8EvTAeAP5IOjzB84gX0R0JC8KOJjOanaN9Y79Sb+yzaIccN5eyYpIvuU5AuKtnxUhhh6bBSMh6KvHu3b4N+Lyxt9CE+KLmrPCFVUZts5lZiyH6f+4sdbMLUMReqlx23ucGvcboQj7qXF/KZsARUSH8q6A3nQWXlOU/rSlkjVRIm8KStaELtFcPiOgG2AnLV1AhkYyqe7dWZ5z5vk6VEpGxvlh6bj2w1bYS43bMamNtuMLVTqyjIh+AgAA///wADwmcAEAAA==",
	"H4sIAAAAAAAA/1SPT0vDQBDF521WMW1APHjx7MmDZ6/SCimUItVb6WEbJxrYJpLdoB6tVeu/zzyyqUjLXmbevnm834k8R1CyIETQvco6yBtB9403kE+CvnjkDPJNRMfyqoDDybRm11jv2J/2KtvMyz7n7ZUsieRHXiIg3vhZEmLokZkzEoK+frpvh90hl7f+DpoQX9acFa6oyrDtXGXGcpj2Ro21ZmYZitBNjdvc49S4vwhFOEiN+09ZBygiOpIPBXQn0/CaovRnLZWsiBJ5V1CyInSIZvIVAZ0AO27pAjI0knH14M7znDPPN6FSMjTOD0rHtR+0wn5q3JZJrbUtX6gSyyKCUiD6DQAA//9O97cGdAEAAA==",
	"H4sIAAAAAAAA/1SPP0sDQRDF582t4iUHYiGCtZWFta0kgQuEIDGFEFJszj0NbO4ku4daGqPGf595ZC8iCdvMvH3zeL9TeY7AsiBEUK3SOsgbQbW115BPguo8mgzyTUQn8srA4Wg8N66y3hl/1iptNSvaJq+vZEkkP/ISAfHGz5IQQ/X1zCAhqOHTfT3s9kxx6++gCPHl3GRTNy2LsO1cZdqaMO31K2v1xBowoZlqt7nHqXZ/EUw4SLX7T1kHMBEdywcDzdE4vGpa+POaSlZEibwzWFaEBtFEviKgEWAHNV1AhkIyKB/cRZ6bzJubUCnpaee7hTNz362F/VS7LROvtS1fqHIkCwBRXpZQw871kJiZQfQbAAD//zNRs9iCAQAA",
	"H4sIAAAAAAAA/1SQz0s7MRTE32Tz/eK2C+LBi2dPHjx7lbawhVKk9iCUHtI1q4V0I5ss6tFatf76m58kFWnJZd7LZMhnTvgpgeAlIYHsWOPArwTZVV6BPwiy96AL8BcRHfOLAA4n01q7xnin/WnHmmZRdXUZX/GKiL/5OQHSrZsVIYUcqoVGRpDjx7so/g90deNvIQnpRa2LuZvbKkz/LgtldFB7w8YYNTMagtDOldue01y53whBOMiV+0vZBAgiOuJ3AbQn03CaeeXPIhWviTJ+ExC8JrSIZvyZAK0AO4p0ARkS2cjeu/Oy1IXX1+FL2UA536+crn0/LvZz5XZMYrPb8YlY3xJAUloLOe5djUPjAAgECIio6ScAAP//Ce9k1Y8BAAA=",
}

func newRetEvent(t *testing.T, s string, res string, err error) Event {
	now := time.Now()
	tt := [2]time.Time{now, now.Add(time.Second)}
	if err != nil {
		return NewReturnEvent(s, Return{Err: err, T: tt})
	}
	raw, err := base64.StdEncoding.DecodeString(res)
	require.NoError(t, err)
	var rs resultset.ResultSet
	require.NoError(t, rs.Decode(raw))
	return NewReturnEvent(s, Return{Res: &rs, T: tt})
}

func TestEventSerde(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event Event
		fail  bool
	}{
		{name: "invalid", event: Event{EventMeta: EventMeta{Kind: "oops"}}, fail: true},
		{name: "block", event: NewBlockEvent("t")},
		{name: "resume", event: NewResumeEvent("t")},
		{name: "invoke", event: NewInvokeEvent("t", Invoke{Stmt: Stmt{"t", "select 1", S_QUERY}})},
		{name: "return", event: newRetEvent(t, "t", "", Error{0, "oops"})},
		{name: "return", event: newRetEvent(t, "t", resultData[0], nil)},
		{name: "return", event: newRetEvent(t, "t", resultData[1], nil)},
		{name: "return", event: newRetEvent(t, "t", resultData[2], nil)},
		{name: "return", event: newRetEvent(t, "t", resultData[3], nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			js, err := json.Marshal(tt.event)
			if tt.fail {
				require.Error(t, err)
				return
			}
			t.Log(">> " + string(js))
			require.NoError(t, err)
			var ev Event
			require.NoError(t, json.Unmarshal(js, &ev))
			if tt.event.Kind == EventInvoke {
				require.Equal(t, tt.event.inv.Stmt, ev.inv.Stmt)
			}
			if tt.event.Kind == EventReturn {
				require.Equal(t, tt.event.ret.T[0].Format(time.RFC3339Nano), ev.ret.T[0].Format(time.RFC3339Nano))
				require.Equal(t, tt.event.ret.T[1].Format(time.RFC3339Nano), ev.ret.T[1].Format(time.RFC3339Nano))
				if tt.event.ret.Err != nil {
					require.Equal(t, tt.event.ret.Err.Error(), ev.ret.Err.Error())
				} else {
					if tt.event.ret.Res.IsExecResult() {
						require.Equal(t, tt.event.ret.Res.ExecResult(), ev.ret.Res.ExecResult())
					} else {
						require.Equal(t, tt.event.ret.Res.DataDigest(), ev.ret.Res.DataDigest())
					}
				}
			}
		})
	}
}
