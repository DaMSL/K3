from setuptools import setup

setup(
    name="Pygments-K3",
    author="P.C. Shyamshankar",
    version="0.1",
    description="Pygments support for the K3 Programming Language.",

    packages=["pygments_k3"],

    install_requires=["pygments"],

    entry_points={
        'pygments.lexers': [
            'K3Lexer = pygments_k3.lexer:K3Lexer',
        ],
    },
)
