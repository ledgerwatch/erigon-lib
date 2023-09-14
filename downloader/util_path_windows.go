package downloader

import "strings"

func isSlash(c uint8) bool {
	return c == '\\' || c == '/'
}

func toUpper(c byte) byte {
	if 'a' <= c && c <= 'z' {
		return c - ('a' - 'A')
	}
	return c
}

// isReservedName reports if name is a Windows reserved device name or a console handle.
// It does not detect names with an extension, which are also reserved on some Windows versions.
//
// For details, search for PRN in
// https://docs.microsoft.com/en-us/windows/desktop/fileio/naming-a-file.
func isReservedName(name string) bool {
	if 3 <= len(name) && len(name) <= 4 {
		switch string([]byte{toUpper(name[0]), toUpper(name[1]), toUpper(name[2])}) {
		case "CON", "PRN", "AUX", "NUL":
			return len(name) == 3
		case "COM", "LPT":
			return len(name) == 4 && '1' <= name[3] && name[3] <= '9'
		}
	}
	// Passing CONIN$ or CONOUT$ to CreateFile opens a console handle.
	// https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea#consoles
	//
	// While CONIN$ and CONOUT$ aren't documented as being files,
	// they behave the same as CON. For example, ./CONIN$ also opens the console input.
	if len(name) == 6 && name[5] == '$' && strings.EqualFold(name, "CONIN$") {
		return true
	}
	if len(name) == 7 && name[6] == '$' && strings.EqualFold(name, "CONOUT$") {
		return true
	}
	return false
}

func isLocal(path string) bool {
	if path == "" {
		return false
	}
	if isSlash(path[0]) {
		// Path rooted in the current drive.
		return false
	}
	if strings.IndexByte(path, ':') >= 0 {
		// Colons are only valid when marking a drive letter ("C:foo").
		// Rejecting any path with a colon is conservative but safe.
		return false
	}
	hasDots := false // contains . or .. path elements
	for p := path; p != ""; {
		var part string
		part, p, _ = cutPath(p)
		if part == "." || part == ".." {
			hasDots = true
		}
		// Trim the extension and look for a reserved name.
		base, _, hasExt := strings.Cut(part, ".")
		if isReservedName(base) {
			if !hasExt {
				return false
			}
			// The path element is a reserved name with an extension. Some Windows
			// versions consider this a reserved name, while others do not. Use
			// FullPath to see if the name is reserved.
			//
			// FullPath will convert references to reserved device names to their
			// canonical form: \\.\${DEVICE_NAME}
			//
			// FullPath does not perform this conversion for paths which contain
			// a reserved device name anywhere other than in the last element,
			// so check the part rather than the full path.
			if p, _ := syscall.FullPath(part); len(p) >= 4 && p[:4] == `\\.\` {
				return false
			}
		}
	}
	if hasDots {
		path = Clean(path)
	}
	if path == ".." || strings.HasPrefix(path, `..\`) {
		return false
	}
	return true
}
