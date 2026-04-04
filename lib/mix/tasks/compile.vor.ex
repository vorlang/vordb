defmodule Mix.Tasks.Compile.Vor do
  @moduledoc "Compiles .vor files in src/vor/ to BEAM modules."

  use Mix.Task.Compiler

  @recursive true
  @vor_source_dir "src/vor"

  @impl true
  def run(_args) do
    vor_dir = Path.join(File.cwd!(), @vor_source_dir)

    if File.dir?(vor_dir) do
      ebin = Mix.Project.compile_path()
      File.mkdir_p!(ebin)

      vor_dir
      |> Path.join("**/*.vor")
      |> Path.wildcard()
      |> Enum.reduce({[], []}, fn vor_file, {diagnostics, compiled} ->
        compile_vor_file(vor_file, ebin, diagnostics, compiled)
      end)
      |> finish()
    else
      {:ok, []}
    end
  end

  defp compile_vor_file(vor_file, ebin, diagnostics, compiled) do
    source = File.read!(vor_file)

    try do
      case Vor.Compiler.compile_string(source) do
        {:ok, result} ->
          beam_file = Path.join(ebin, "#{result.module}.beam")
          File.write!(beam_file, result.binary)
          Mix.shell().info("Compiled #{vor_file} → #{result.module}")
          {diagnostics, [result.module | compiled]}

        {:error, errors} ->
          new_diags = errors_to_diagnostics(vor_file, errors)
          {diagnostics ++ new_diags, compiled}
      end
    rescue
      e ->
        diag = %Mix.Task.Compiler.Diagnostic{
          file: vor_file,
          severity: :error,
          message: "Vor compilation crashed: #{Exception.message(e)}",
          position: 0,
          compiler_name: "vor"
        }

        Mix.shell().error("Failed to compile #{vor_file}: #{Exception.message(e)}")
        {[diag | diagnostics], compiled}
    end
  end

  defp errors_to_diagnostics(vor_file, errors) when is_list(errors) do
    Enum.map(errors, &error_to_diagnostic(vor_file, &1))
  end

  defp errors_to_diagnostics(vor_file, error) do
    [error_to_diagnostic(vor_file, error)]
  end

  defp error_to_diagnostic(vor_file, error) do
    msg = format_error(error)
    Mix.shell().error("Vor error in #{vor_file}: #{msg}")

    %Mix.Task.Compiler.Diagnostic{
      file: vor_file,
      severity: :error,
      message: msg,
      position: 0,
      compiler_name: "vor"
    }
  end

  defp finish({[], _compiled}), do: {:ok, []}
  defp finish({diagnostics, _compiled}), do: {:error, diagnostics}

  defp format_error(%{message: msg}) when is_binary(msg), do: msg
  defp format_error(%{type: type, message: msg}), do: "#{type}: #{msg}"
  defp format_error(err) when is_binary(err), do: err
  defp format_error(err), do: inspect(err)
end
