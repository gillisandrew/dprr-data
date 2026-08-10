import { defineConfig } from "vite-plus"
import { devtools } from "@tanstack/devtools-vite"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import viteReact from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"
import { staticDataPlugin } from "./src/build/static-data-plugin.ts"
import { sparqlDumpPlugin } from "./src/build/sparql-dump-plugin.ts"

const config = defineConfig({
  base: "/dprr-data/",
  // Vite resolves the `@/*` alias from tsconfig.json natively; the
  // vite-tsconfig-paths plugin is no longer needed.
  resolve: { tsconfigPaths: true },
  lint: { options: { typeAware: true, typeCheck: true } },
  fmt: {
    endOfLine: "lf",
    semi: false,
    singleQuote: false,
    tabWidth: 2,
    trailingComma: "es5",
    printWidth: 80,
    sortTailwindcss: {
      stylesheet: "src/styles.css",
      functions: ["cn", "cva"],
    },
    sortPackageJson: false,
    ignorePatterns: [
      "package-lock.json",
      "pnpm-lock.yaml",
      "yarn.lock",
      "src/routeTree.gen.ts",
    ],
  },
  plugins: [
    staticDataPlugin(),
    sparqlDumpPlugin(),
    devtools(),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
        crawlLinks: true,
        // The crawler follows the /sparql page's dump download link and
        // would overwrite public/dump/dprr.nt.gz in dist with a
        // transport-decompressed copy; only HTML routes should prerender.
        filter: (page) => !page.path.includes("/dump/"),
      },
      sitemap: {
        enabled: true,
        // The generator joins host + route path and is unaware of Vite's
        // `base`, so the project-page prefix has to live in the host.
        host: "https://gillisandrew.github.io/dprr-data",
      },
    }),
    viteReact(),
  ],
})

export default config
