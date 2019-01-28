defmodule Mariaex.Mixfile do
  use Mix.Project

  def project do
    [app: :mariaex,
     version: "0.9.1",
     elixir: "~> 1.3",
     deps: deps(),
     name: "Mariaex",
     source_url: "https://github.com/xerions/mariaex",
     test_coverage: [tool: Coverex.Task, coveralls: true],
     description: description(),
     package: package(),
     docs: [
      main: "Mariaex",
      extras: ["README.md"]
     ]
    ]
  end

  # Configuration for the OTP application
  def application do
    [applications: [:logger, :crypto, :decimal, :db_connection]]
  end

  defp deps do
    [{:decimal, "~> 1.2"},
     {:db_connection, "~> 2.0"},
     {:coverex, "~> 1.4.10", only: :test},
     {:ex_doc, ">= 0.0.0", only: :dev},
     {:poison, ">= 0.0.0", optional: true}]
  end

  defp description do
    "Pure elixir database driver for MariaDB / MySQL."
  end

  defp package do
    [maintainers: ["Dmitry Russ(Aleksandrov)"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/xerions/mariaex"}]
  end
end
