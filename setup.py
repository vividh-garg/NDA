import setuptools
VERSION = '0.0.6' 
DESCRIPTION = 'Python Package for working with NDA files'



with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='lime_nda',
    version=VERSION,
    author='lime_celltesting',
    author_email='vividh.garg@lime.ai',
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/vividh-garg/NDA',
    project_urls = {
        "Bug Tracker": "https://github.com/vividh-garg/NDA/issues"
    },
    license='MIT',
    packages=['lime_nda'],
    install_requires=["pandas>=1.5.2","numpy>=1.23.5"],
)
