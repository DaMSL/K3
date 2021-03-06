from pygments.token import Comment, Keyword, Literal, Operator, Text
from pygments.lexer import RegexLexer

class K3Lexer(RegexLexer):
    name = 'K3'
    aliases = ['k3']
    filenames = ['*.k3']

    keywords = {
        'preprocessor': "include",

        "declaration": [
            "annotation", "declare", "feed", "provides", "requires", "source", "trigger"
        ],

        "type": ["bool", "int", "real", "string", "option", "ind", "collection"],

        "expression": [
            "if", "then", "else", "let", "in", "case", "of", "bind", "as", "some", "none",
            "true", "false"
        ]
    }

    tokens = {
        'root': [
            (r'//.*?$', Comment),
            (r'\b(' + r'|'.join(keywords["preprocessor"]) + r')\s*\b(?!\.)', Keyword.Namespace),
            (r'\b(' + r'|'.join(keywords["declaration"]) + r')\s*\b(?!\.)', Keyword.Declaration),
            (r'\b(' + r'|'.join(keywords["type"]) + r')\s*\b(?!\.)', Keyword.Type),
            (r'\b(' + r'|'.join(keywords["expression"]) + r')\s*\b(?!\.)', Keyword.Reserved),

            (r'[+\-*/<>=!&|\\():;,]+', Operator),

            (r'[0-9]+(\.[0-9]*)?', Literal.Number),
            (r'"[^"]*"', Literal.String),
            (r'\b([_a-zA-Z][_a-zA-Z0-9]*)\b', Text),
            (r'\s+', Text.Whitespace)
        ],
    }
