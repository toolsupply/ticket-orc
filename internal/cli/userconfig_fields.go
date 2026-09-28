package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

func validateConfigJSONFields(data []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return err
	}
	if err := validateObjectFields(root, "config", map[string]struct{}{"version": {}, "id": {}, "local_dir": {}, "default_role": {}, "defaults": {}, "roles": {}, "review": {}, "repositories": {}, "workers": {}, "supervisor": {}}); err != nil {
		return err
	}
	if raw := root["review"]; raw != nil {
		if err := validateNestedConfigObject(raw, "review", map[string]struct{}{"skip_tags": {}}); err != nil {
			return err
		}
	}
	if raw := root["defaults"]; raw != nil {
		if err := validateNestedConfigObject(raw, "defaults", defaultConfigFields); err != nil {
			return err
		}
	}
	if raw := root["roles"]; raw != nil {
		var roles map[string]json.RawMessage
		if err := json.Unmarshal(raw, &roles); err != nil {
			return err
		}
		roleNames := make([]string, 0, len(roles))
		for role := range roles {
			roleNames = append(roleNames, role)
		}
		sort.Strings(roleNames)
		for _, role := range roleNames {
			if err := validateNestedConfigObject(roles[role], "roles."+role, roleConfigFields); err != nil {
				return err
			}
		}
	}
	if raw := root["workers"]; raw != nil {
		var workers map[string]json.RawMessage
		if err := json.Unmarshal(raw, &workers); err != nil {
			return err
		}
		workerNames := make([]string, 0, len(workers))
		for name := range workers {
			workerNames = append(workerNames, name)
		}
		sort.Strings(workerNames)
		for _, name := range workerNames {
			if err := validateNestedConfigObject(workers[name], "workers."+name, workerConfigFields); err != nil {
				return err
			}
		}
	}
	if raw := root["repositories"]; raw != nil {
		var repositories map[string]json.RawMessage
		if err := json.Unmarshal(raw, &repositories); err != nil {
			return err
		}
		names := make([]string, 0, len(repositories))
		for name := range repositories {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := validateNestedConfigObject(repositories[name], "repositories."+name, map[string]struct{}{"repository": {}, "ticket": {}}); err != nil {
				return err
			}
		}
	}
	if raw := root["supervisor"]; raw != nil {
		if err := validateNestedConfigObject(raw, "supervisor", map[string]struct{}{"startup_groups": {}, "listen": {}, "port": {}, "endpoint_key": {}}); err != nil {
			return err
		}
	}
	return nil
}

var defaultConfigFields = configFieldSet("required_skills", "harness", "model", "reasoning", "session_policy", "session_cleanup", "minimum_reuse_context_percent", "review_completion", "ticket_prompt", "working_dir", "repository", "ticket", "output", "codex", "pi", "claude")
var roleConfigFields = configFieldSet("ticket_queue", "nudge_prompt", "groups", "required_skills", "harness", "actor", "model", "reasoning", "max_bounces", "session_policy", "session_cleanup", "minimum_reuse_context_percent", "working_dir", "repository", "ticket", "output", "review_completion", "ticket_prompt", "codex", "pi", "claude")
var workerConfigFields = configFieldSet("role", "groups", "required_skills", "harness", "actor", "model", "reasoning", "max_bounces", "session_policy", "session_cleanup", "minimum_reuse_context_percent", "working_dir", "repository", "ticket", "output", "review_completion", "ticket_prompt", "codex", "pi", "claude")

func configFieldSet(fields ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		result[field] = struct{}{}
	}
	return result
}

func validateNestedConfigObject(raw json.RawMessage, path string, allowed map[string]struct{}) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if err := validateObjectFields(object, path, allowed); err != nil {
		return err
	}
	for _, nested := range []string{"codex", "pi", "claude", "ticket"} {
		if value := object[nested]; value != nil {
			fields := map[string]struct{}{}
			switch nested {
			case "codex":
				fields["sandbox"] = struct{}{}
			case "pi":
				fields["provider"] = struct{}{}
			case "claude":
				fields["permission_mode"] = struct{}{}
			case "ticket":
				fields["config"] = struct{}{}
				fields["scope"] = struct{}{}
			}
			if err := validateNestedConfigObject(value, path+"."+nested, fields); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateObjectFields(object map[string]json.RawMessage, path string, allowed map[string]struct{}) error {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[key]; ok {
			continue
		}
		message := fmt.Sprintf("unknown field at %s.%s", path, key)
		if suggestion := nearestConfigField(key, allowed); suggestion != "" {
			message += fmt.Sprintf("; did you mean %s?", suggestion)
		}
		return errors.New(message)
	}
	return nil
}

func nearestConfigField(value string, allowed map[string]struct{}) string {
	best, distance := "", 3
	for candidate := range allowed {
		current := configEditDistance(value, candidate)
		if current < distance {
			best, distance = candidate, current
		}
	}
	return best
}

func configEditDistance(left, right string) int {
	row := make([]int, len(right)+1)
	for i := range row {
		row[i] = i
	}
	for i, l := range left {
		next := make([]int, len(row))
		next[0] = i + 1
		for j, r := range right {
			cost := 0
			if l != r {
				cost = 1
			}
			next[j+1] = minConfigInt(next[j]+1, row[j+1]+1, row[j]+cost)
		}
		row = next
	}
	return row[len(right)]
}

func minConfigInt(values ...int) int {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
