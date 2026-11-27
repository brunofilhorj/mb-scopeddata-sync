using System.Diagnostics;

namespace MB.ScopedDataSync.Containers.Docker;

internal static class DockerAvailability
{
    public static bool IsAvailable { get; } = Probe();

    private static bool Probe()
    {
        try
        {
            using var process = new Process();
            process.StartInfo = new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            process.OutputDataReceived += static (_, _) => { };
            process.ErrorDataReceived += static (_, _) => { };
            if (!process.Start())
            {
                return false;
            }

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            if (!process.WaitForExit(3000))
            {
                process.Kill(entireProcessTree: true);
                return false;
            }

            return process.ExitCode == 0;
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            return false;
        }
    }
}

public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!DockerAvailability.IsAvailable)
        {
            Skip = "Docker is not available.";
        }
    }
}
