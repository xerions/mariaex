defmodule Mariaex.Mixfile do
  use Mix.Project

  def project do
    [app: :mariaex,
     version: "0.4.3",
     elixir: "~> 1.0",
     deps: deps,
     name: "Mariaex",
     source_url: "https://github.com/liveforeverx/mariaex",
     test_coverage: [tool: Coverex.Task, coveralls: true],
     description: description,
     package: package]
  end

  # Configuration for the OTP application
  def application do
    [applications: [:logger, :decimal]]
  end

  defp deps do
    [{:decimal, "~> 1.0"},
     {:coverex, "~> 1.4.1", only: :test}]
  end

  defp description do
    "Pure elixir database driver for MariaDB / MySQL."
  end

  defp package do
    [contributors: ["Dmitry Russ(Aleksandrov)"],
     links: %{"Github" => "https://github.com/xerions/mariaex"}]
  end
end
