package utils

import "testing"

func Test_QuoteTo(t *testing.T) {
	testCases := []struct {
		name string
		args string
		want string
	}{
		{
			name: "none `",
			args: "abc",
			want: "`abc`",
		},
		{
			name: "middle contain double `",
			args: "ab``c",
			want: "`ab``c`",
		},
		{
			name: "middle contain `",
			args: "ab`c",
			want: "`ab``c`",
		},
		{
			name: "suffix contain `",
			args: "abc`",
			want: "`abc```",
		},
		{
			name: "prefix contain `",
			args: "`abc",
			want: "`abc`",
		},
		{
			name: "prefix suffix contain `",
			args: "`abc`",
			want: "`abc`",
		},
		{
			name: "contain .",
			args: "ab.c",
			want: "`ab`.`c`",
		},
		{
			name: "contain .`",
			args: "`ab`.`c`",
			want: "`ab`.`c`",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := Quote(tc.args)
			if got != tc.want {
				t.Errorf("QuoteTo() = %v, want %v", got, tc.want)
			}
		})
	}

}
