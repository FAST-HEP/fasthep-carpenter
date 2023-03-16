from __future__ import annotations

import copy
from typing import TypeVar

T = TypeVar('T')

# trying to immitate the font from
# https://fonts.google.com/specimen/Press+Start+2P?preview.text=FAST-HEP&preview.text_type=custom


logo_f = """
█████████╗
███╔═════╝
███║
███████╗
███╔═══╝
███║
███║
╚══╝
"""

logo_a = """
   █████╗
 ███╔══███╗
███╔╝   ███╗
███║    ███║
███████████║
███║    ███║
███║    ███║
╚══╝    ╚══╝

"""

logo_s = """
  ██████╗
███╔═══███╗
███║   ╚══╝
 ╚██████╗
  ╚════███╗
███    ███║
╚═██████╔═╝
  ╚═════╝
"""

logo_t = """
███████████╗
╚═══███╔═══╝
    ███║
    ███║
    ███║
    ███║
    ███║
    ╚══╝
"""

logo_h = """
███╗    ███╗
███║    ███║
███║    ███║
███████████║
███╔════███║
███║    ███║
███║    ███║
╚══╝    ╚══╝
"""

logo_e = """
██████████╗
███╔══════╝
███║
█████████╗
███╔═════╝
███║
██████████╗
╚═════════╝
"""

logo_p = """
█████████╗
███╔════███╗
███║    ███║
█████████╔═╝
███╔═════╝
███║
███║
╚══╝
"""

logo_stripes = """

▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

      ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

   ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

"""

logo_runner = """
             ▄███▄
       ▄▄▄▄▄  ▀█▀ ▄
     ▄█▀  ████▄▄▄▀▀
     █   ███
       ▄███
 ▄▄▄▄▄██▀███▄
 ▐▀▀▀▀▀▀   ██
           █▀
           █
           ▀▀
"""


def overlay(piece1, piece2):
    """
    Overlays two ASCII art pieces by preferring non-empty characters of piece1.

    Arguments:
    piece1 -- the first ASCII art piece
    piece2 -- the second ASCII art piece

    Returns:
    A new ASCII art piece that is the result of overlaying piece1 and piece2.
    """
    # Split the input pieces into lines
    lines1 = piece1.split('\n')
    lines2 = piece2.split('\n')

    # Determine the maximum number of lines and line length in the pieces
    max_lines = max(len(lines1), len(lines2))
    max_line_len = max(len(line) for line in lines1 + lines2)

    # Create a list of lines for the output piece
    output_lines = []

    # Overlay the lines of the two pieces
    for i in range(max_lines):
        # Get the i-th line of each piece
        line1 = lines1[i] if i < len(lines1) else ''
        line2 = lines2[i] if i < len(lines2) else ''

        # Pad the lines to the maximum length
        line1 = line1.ljust(max_line_len)
        line2 = line2.ljust(max_line_len)

        # Overlay the characters of the two lines
        output_line = ''
        for j in range(max_line_len):
            if line1[j] != ' ':
                output_line += line1[j]
            else:
                output_line += line2[j]
        output_lines.append(output_line)

    # Join the output lines into a single string and return it
    return '\n'.join(output_lines)


class MultiLineText:
    def __init__(self: T, text):
        if isinstance(text, MultiLineText):
            self.lines = copy.deepcopy(text.lines)
            return
        self.lines = text.split("\n")
        width = self.width
        self.lines = [line.ljust(width) for line in self.lines]

    def __getitem__(self: T, index: int):
        return self.lines[index]

    def __len__(self: T):
        return max(len(line) for line in self.lines)

    @property
    def height(self: T) -> int:
        return len(self.lines)

    @property
    def width(self: T) -> int:
        return len(self)

    def pad_height(self: T, top: int = 0, bottom: int = 0) -> None:
        self.lines = [" " * self.width] * top + self.lines + [" " * self.width] * bottom

    def append(self, text: T, spacing: int = 2) -> None:
        diff_height = self.height - text.height
        if diff_height < 0:
            self.pad_height(bottom=abs(diff_height))

        lines = []
        for l1, l2 in zip(self.lines, text.lines):
            l1 += " " * spacing + l2
            lines.append(l1)
        self.lines = lines

    def overlay(self: T, text: T, offset: int = 0) -> T:
        result = MultiLineText(copy.deepcopy(self))
        diff_height = result.height - text.height

        if diff_height < 0:
            padding = abs(diff_height)
            # split padding in half for both top and bottom
            padding_top = padding // 2
            padding_bottom = padding - padding_top
            result.pad_height(top=padding_top, bottom=padding_bottom)

        for i, line in enumerate(text.lines):
            start = offset
            end = offset + len(line)
            new_line = result[i][:start]
            for j, char in enumerate(line):
                if char != " ":
                    new_line += char
                else:
                    new_line += result.lines[i][start + j]
            if len(result.lines[i]) > end:
                new_line += result.lines[i][end:]
            result.lines[i] = new_line
        return result

    def __str__(self: T):
        return "\n".join(self.lines)


def merge_pieces(pieces, spacing=2):
    """
    Merges a list of ASCII art pieces into a single piece.
    """
    # Merge the pieces by overlaying them one by one
    result = MultiLineText(pieces[0])
    for piece in pieces[1:]:
        result.append(MultiLineText(piece), spacing=spacing)

    return result


def main():
    space = "\n".join([" " * 11] * 9)
    merged = merge_pieces([space, logo_f, logo_a, logo_s, logo_t, space, logo_h, logo_e, logo_p])
    overlayed = merged.overlay(MultiLineText(logo_runner), offset=59)
    print(overlayed)


if __name__ == '__main__':
    main()
