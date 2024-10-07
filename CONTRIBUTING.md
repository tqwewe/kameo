# Contributing to Kameo

Thank you for considering contributing to Kameo! We welcome all contributions, whether they are bug reports, new features, improvements to documentation, or anything else that can help improve the project.

Please take a moment to review the guidelines below to ensure that your contributions are in line with the project’s standards.

## How to Contribute

### Reporting Bugs
If you've found a bug or an issue with Kameo, we would appreciate if you could:

1. **Search Existing Issues**: Please check the [issues page](https://github.com/tqwewe/kameo/issues) to see if your bug has already been reported. If it has, feel free to add additional information as a comment.
2. **Create a New Issue**: If the issue hasn’t been reported, [create a new issue](https://github.com/tqwewe/kameo/issues/new) with the following details:
   - A clear title describing the bug.
   - Steps to reproduce the bug.
   - Expected behavior and what actually happened.
   - Relevant environment details (OS, Rust version, etc.).
   - Any other relevant information or screenshots.

### Suggesting Features or Enhancements
We are always open to new ideas! If you have a feature or enhancement request, please:

1. **Search Existing Feature Requests**: Look through the [existing issues](https://github.com/tqwewe/kameo/issues) to see if someone has already suggested a similar feature.
2. **Open a New Feature Request**: If your idea hasn’t been suggested yet, [open a new issue](https://github.com/tqwewe/kameo/issues/new) describing:
   - The motivation for the feature.
   - How you envision the feature working.
   - Any additional context or examples that would be helpful.

### Pull Requests
We welcome pull requests for bug fixes, new features, or improvements! Here’s how you can contribute:

1. **Fork the Repository**: Start by [forking the repository](https://github.com/tqwewe/kameo/fork) and cloning it locally.
   ```bash
   git clone https://github.com/tqwewe/kameo.git
   cd kameo
   ```
2. **Create a New Branch**: Always create a new branch for your changes.
   ```bash
   git checkout -b my-feature-branch
   ```
3. **Make Changes**: Implement your changes, making sure your code follows the project’s style and is well-documented.
4. **Test Your Changes**: If applicable, run the tests to ensure your changes don’t break anything. Add new tests if you’ve added a new feature or fixed a bug.
   ```bash
   cargo test
   ```
5. **Use Conventional Commits**: We follow the [Conventional Commits](https://www.conventionalcommits.org) specification for commit messages. This ensures that commit messages are structured and informative. The format is:
   ```bash
   <type>[optional scope]: <description>

   [optional body]

   [optional footer(s)]
   ```
   For example:
   ```
   feat(actor): add support for remote actors
   fix(pubsub): resolve issue with message broadcasting
   ```
   Common types include:
   - `feat`: A new feature.
   - `fix`: A bug fix.
   - `docs`: Documentation-only changes.
   - `style`: Changes that do not affect the meaning of the code (formatting, missing semicolons, etc.).
   - `refactor`: A code change that neither fixes a bug nor adds a feature.
   - `test`: Adding or modifying tests.

   If your changes introduce a **breaking change**, add an exclamation mark (`!`) after the type:
   ```
   feat(actor)!: change remote actor messaging system
   ```
   Breaking changes should also be mentioned in the commit body or footer for clarity.

6. **Commit and Push**: Commit your changes with a meaningful message and push your branch.
   ```bash
   git add .
   git commit -m "Description of changes"
   git push origin my-feature-branch
   ```
7. **Create a Pull Request**: Go to the GitHub page of your forked repo and click on “New Pull Request”. In your pull request, provide a detailed explanation of your changes, referencing the issue if applicable.

### Code Guidelines

- **Follow Rust’s Style**: Ensure your code follows Rust’s conventions. You can use `cargo fmt` to automatically format your code.
- **Document Your Code**: Ensure that all public items (functions, structs, modules) have proper Rustdoc comments.
- **Write Tests**: When adding new functionality, write tests to ensure the new code works as expected.

### Code of Conduct
By contributing, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

---

Thank you for contributing to Kameo! Your help makes this project better.
