using System.Diagnostics;

namespace MB.ScopedDataSync.Abstractions.Instrumentation;

public readonly struct InstrumentationLease : IDisposable
{
    private readonly Activity? _activity;

    public InstrumentationLease(Activity? activity) => _activity = activity;

    public void SetOutcome(string outcome) => _activity?.SetTag("entity.outcome", outcome);

    public void SetError(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (_activity is null)
        {
            return;
        }

        _activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        _activity.AddException(exception);
        _activity.SetTag("entity.outcome", "error");
    }

    public void Dispose() => _activity?.Dispose();
}
