import { authedFetch } from './client-lib'

type Options = {
  baseUrl: string
  method: string
  path: string
  body?: string
  contentType?: string
  rawHeaders: Array<[string, string]>
}

function usage(): never {
  console.error(`Usage:
  pnpm dlx tsx scripts/client.ts [METHOD] <PATH> [options]

Examples:
  pnpm dlx tsx scripts/client.ts GET /api/sandbox
  pnpm dlx tsx scripts/client.ts POST /api/sandbox --json '{"name":"sandbox-1"}'
  pnpm dlx tsx scripts/client.ts /toolbox/sandbox-1/toolbox/files?path=/

Options:
  --url <base>           Base URL. Default: http://localhost:7935
  --json <payload>       JSON request body. Sets Content-Type: application/json
  --data <payload>       Raw request body
  --content-type <type>  Content-Type for --data
  --header <name:value>  Additional header; may be repeated

Auth:
  CONTROL_SECRET is read from the environment first, then cf-demo/.dev.vars.
  The client always sends X-Control-Secret automatically.`)
  process.exit(1)
}

function parseArgs(argv: string[]): Options {
  if (argv.length === 0) {
    usage()
  }

  const args = [...argv]
  let method = 'GET'
  let path = ''

  const first = args[0]
  if (/^[A-Z]+$/.test(first)) {
    method = args.shift()!
  }

  path = args.shift() ?? ''
  if (!path) {
    usage()
  }

  const options: Options = {
    baseUrl: 'http://localhost:7935',
    method,
    path,
    rawHeaders: [],
  }

  while (args.length > 0) {
    const flag = args.shift()!
    switch (flag) {
      case '--url':
        options.baseUrl = args.shift() ?? usage()
        break
      case '--json':
        options.body = args.shift() ?? usage()
        options.contentType = 'application/json'
        break
      case '--data':
        options.body = args.shift() ?? usage()
        break
      case '--content-type':
        options.contentType = args.shift() ?? usage()
        break
      case '--header': {
        const header = args.shift() ?? usage()
        const idx = header.indexOf(':')
        if (idx === -1) {
          throw new Error(`invalid header ${JSON.stringify(header)}; expected name:value`)
        }
        options.rawHeaders.push([
          header.slice(0, idx).trim(),
          header.slice(idx + 1).trim(),
        ])
        break
      }
      default:
        throw new Error(`unknown argument: ${flag}`)
    }
  }

  return options
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  const headers = new Headers()
  if (options.contentType) {
    headers.set('Content-Type', options.contentType)
  }
  for (const [name, value] of options.rawHeaders) {
    headers.set(name, value)
  }

  const response = await authedFetch(options.path, {
    method: options.method,
    headers,
    body: options.body,
  }, { baseUrl: options.baseUrl })

  const contentType = response.headers.get('content-type') ?? ''
  const body = await response.text()

  console.error(`${response.status} ${response.statusText}`)
  if (contentType.includes('application/json')) {
    try {
      console.log(JSON.stringify(JSON.parse(body), null, 2))
    } catch {
      process.stdout.write(body)
    }
  } else {
    process.stdout.write(body)
  }

  if (!response.ok) {
    process.exitCode = 1
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exit(1)
})
