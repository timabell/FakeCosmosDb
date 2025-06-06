#!/bin/sh -v
set -e # exit on error

if ! command -v dotnet-outdated
then
	# https://github.com/dotnet-outdated/dotnet-outdated
	dotnet tool install --global dotnet-outdated-tool
fi

dotnet outdated -u --no-restore
dotnet test
git commit --include '*.csproj' --message "Nuget update/upgrade"
