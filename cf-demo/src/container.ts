import { Container, type StopParams } from '@cloudflare/containers'

export class SandboxContainer extends Container<Env> {
  defaultPort = 8080
  sleepAfter = '24h' as const
  enableInternet = true

  constructor(ctx: DurableObjectState, env: Env) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    super(ctx as any, env)
    this.envVars = {
      R2_ENDPOINT: env.R2_ENDPOINT,
      R2_BUCKET: env.R2_BUCKET,
      R2_ACCESS_KEY: env.R2_ACCESS_KEY,
      R2_SECRET_KEY: env.R2_SECRET_KEY,
      AXIOM_TOKEN: env.AXIOM_TOKEN,
      AXIOM_DATASET: env.AXIOM_DATASET,
      CONTAINER_DO_ID: ctx.id.toString(),
      SANDBOX_MODE: env.SANDBOX_MODE,
    }
  }

  private get id() {
    return this.ctx.id.toString()
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/stop') {
      await this.destroy()
      return new Response('stopped')
    }
    return this.containerFetch(request, this.defaultPort)
  }

  override onStart() {
    console.log('[container]', { event: 'start', id: this.id })
  }

  override async onStop(params: StopParams) {
    console.log('[container]', { event: 'stop', id: this.id, ...params })
    try {
      const scheduler = this.env.SCHEDULER.get(this.env.SCHEDULER.idFromName('scheduler'))
      await scheduler.fetch(
        new Request('http://scheduler/_internal/container-died', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ containerId: this.id }),
        }),
      )
    } catch (error) {
      console.error('[container]', { event: 'stop_notify_failed', id: this.id, error })
    }
  }

  override async onActivityExpired() {
    console.log('[container]', { event: 'activity_expired', id: this.id })
    await this.stop()
  }

  override onError(error: unknown) {
    console.error('[container]', { event: 'error', id: this.id, error })
    throw error
  }
}
