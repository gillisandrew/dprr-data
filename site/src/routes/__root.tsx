import {
  HeadContent,
  Link,
  Outlet,
  Scripts,
  createRootRoute,
} from "@tanstack/react-router"
import type { ErrorComponentProps } from "@tanstack/react-router"
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools"
import { TanStackDevtools } from "@tanstack/react-devtools"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"

import appCss from "../styles.css?url"
// Hashed asset URL — the SVG lives in src/assets (single source with the
// inlined nav logo), not public/, so Vite fingerprints it like any asset.
import iconSvgHref from "@/assets/icon.svg"

// Inline, blocking, and placed before HeadContent so it runs before any
// stylesheet or paint on every prerendered page — avoids a flash of the
// wrong theme. Keep in sync with theme-toggle.tsx's storage contract
// ("theme": "light" | "dark" in localStorage).
const noFlashThemeScript = `try{var t=localStorage.getItem("theme");var d=t==="dark"||(t!=="light"&&matchMedia("(prefers-color-scheme: dark)").matches);document.documentElement.classList.toggle("dark",d)}catch(e){}`

export const Route = createRootRoute({
  head: () => ({
    meta: [
      {
        charSet: "utf-8",
      },
      {
        name: "viewport",
        content: "width=device-width, initial-scale=1",
      },
      {
        title: "DPRR — Digital Prosopography of the Roman Republic",
      },
      {
        name: "description",
        content:
          "Search and browse 4,876 persons from the Roman Republic (509–31 BC)",
      },
    ],
    links: [
      {
        rel: "stylesheet",
        href: appCss,
      },
      // Explicit icon links are required: without them browsers request
      // /favicon.ico from the origin root, which this /dprr-data/ project
      // page never serves.
      {
        rel: "icon",
        type: "image/svg+xml",
        href: iconSvgHref,
      },
      {
        rel: "icon",
        sizes: "32x32 16x16",
        href: `${import.meta.env.BASE_URL}favicon.ico`,
      },
      {
        rel: "apple-touch-icon",
        href: `${import.meta.env.BASE_URL}apple-touch-icon.png`,
      },
      {
        rel: "manifest",
        href: `${import.meta.env.BASE_URL}manifest.json`,
      },
    ],
  }),
  shellComponent: RootDocument,
  component: RootLayout,
  // Caught here (not in each leaf route) so SiteHeader/SiteFooter from
  // RootLayout keep rendering — only the Outlet's content is swapped out.
  errorComponent: RootError,
  notFoundComponent: RootNotFound,
})

function BackToSearch() {
  return (
    <Link to="/" className="text-accent-ink hover:underline">
      Back to search
    </Link>
  )
}

function RootError(_props: ErrorComponentProps) {
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-2xl font-bold">
        Something went wrong loading this page.
      </h1>
      <p className="mt-4">
        <BackToSearch />
      </p>
    </div>
  )
}

function RootNotFound() {
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-2xl font-bold">
        Not found — this record doesn't exist.
      </h1>
      <p className="mt-4">
        <BackToSearch />
      </p>
    </div>
  )
}

function RootLayout() {
  return (
    <>
      <SiteHeader />
      <Outlet />
      <SiteFooter />
    </>
  )
}

function RootDocument({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        {/* Raw script (not head()'s `scripts`) so it renders ahead of
            HeadContent in the shell markup. */}
        <script dangerouslySetInnerHTML={{ __html: noFlashThemeScript }} />
        <HeadContent />
      </head>
      <body>
        {children}
        {import.meta.env.DEV && (
          <TanStackDevtools
            config={{
              position: "bottom-right",
            }}
            plugins={[
              {
                name: "Tanstack Router",
                render: <TanStackRouterDevtoolsPanel />,
              },
            ]}
          />
        )}
        <Scripts />
      </body>
    </html>
  )
}
