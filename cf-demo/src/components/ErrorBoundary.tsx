import { Component, type ReactNode } from 'react'

interface State {
  error: Error | null
}

export default class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  render() {
    if (this.state.error) {
      return (
        <div className="p-4 m-4 rounded border border-destructive bg-destructive/10 text-sm font-mono">
          <p className="font-bold text-destructive mb-2">Render Error</p>
          <p className="text-foreground whitespace-pre-wrap">{this.state.error.message}</p>
          <pre className="mt-2 text-xs text-muted-foreground overflow-auto max-h-48">
            {this.state.error.stack}
          </pre>
          <button
            className="mt-3 px-3 py-1 text-xs rounded bg-muted hover:bg-accent"
            onClick={() => this.setState({ error: null })}
          >
            Retry
          </button>
        </div>
      )
    }
    return this.props.children
  }
}
