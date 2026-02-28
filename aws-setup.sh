# bin/bash
# Download the AWS CLI installer.
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /

# Setup AWS CLI Zsh completions
if command -v aws_completer &> /dev/null; then
    echo "Adding AWS CLI completions to .zshrc..."
    cat <<EOF >> ${ZDOTDIR:-$HOME}/.zshrc

# AWS CLI tab-completion
complete -C "$(which aws_completer)" aws
EOF
    echo "AWS CLI completions configured."
else
    echo "aws_completer not found. AWS CLI completions not enabled."
fi
