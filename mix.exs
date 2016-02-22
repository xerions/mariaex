defmodule Mariaex.Mixfile do
  use Mix.Project

  def project do
    [app: :mariaex,
     version: "0.6.4",
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
    [applications: [:logger, :decimal, :connection]]
  end

  defp deps do
    [{:decimal, "~> 1.0"},
     {:connection, "~> 1.0.0"},
     {:coverex, "~> 1.4.3", only: :test}]
  end

  defp description do
    "Pure elixir database driver for MariaDB / MySQL."
  end

  defp package do
    [maintainers: ["Dmitry Russ(Aleksandrov)"],
     links: %{"Github" => "https://github.com/xerions/mariaex"}]
  end
end
