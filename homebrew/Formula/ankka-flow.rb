# The flow CLI, for `brew install thinkmorestupidless/tap/ankka-flow`.
#
# Canonical here, with the version and checksums as placeholders. The release workflow's `homebrew`
# job writes the tag's version into each url (0.0.0 nowhere else: Homebrew reads the version from
# the url, and `brew audit --strict` refuses a `version` line that repeats it) and the checksum
# published beside each native build on the tag's GitHub release — the zero line for each platform
# is found by the comment on it — and commits the result as Formula/ankka-flow.rb in
# thinkmorestupidless/homebrew-tap, beside ankka's own formula. Changes go here; the tap is written
# only by that job.
class AnkkaFlow < Formula
  desc "Command-line client for ankka-flow, streaming pipelines beside ankka"
  homepage "https://flow.ankka.cloud/"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/thinkmorestupidless/ankka-flow/releases/download/v0.0.0/ankka-flow-cli-0.0.0-macos-arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # macos-arm64
    end
    on_intel do
      url "https://github.com/thinkmorestupidless/ankka-flow/releases/download/v0.0.0/ankka-flow-cli-0.0.0-macos-x64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # macos-x64
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/thinkmorestupidless/ankka-flow/releases/download/v0.0.0/ankka-flow-cli-0.0.0-linux-arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # linux-arm64
    end
    on_intel do
      url "https://github.com/thinkmorestupidless/ankka-flow/releases/download/v0.0.0/ankka-flow-cli-0.0.0-linux-x64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # linux-x64
    end
  end

  def install
    bin.install "flow"
  end

  test do
    assert_match "flow #{version}, protocol", shell_output("#{bin}/flow version")
  end
end
