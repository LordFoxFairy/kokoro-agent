import { FailedPreconditionError } from "../../../application/errors.js";
import { assertCustomApiEnvironmentName } from "./custom-api-secret-crypto.js";

// Credentials may configure clients, not replace the sandbox's executable/runtime.
const reserved = new Set([
  "PATH",
  "HOME",
  "USER",
  "LOGNAME",
  "SHELL",
  "IFS",
  "ENV",
  "BASH_ENV",
  "SHELLOPTS",
  "BASHOPTS",
  "CDPATH",
  "PWD",
  "OLDPWD",
  "TMPDIR",
  "TMP",
  "TEMP",
  "LANG",
  "LC_ALL",
  "TZ",
  "ZDOTDIR",
  "NODE_OPTIONS",
  "NODE_PATH",
  "RUBYOPT",
  "RUBYLIB",
  "PERL5OPT",
  "PERL5LIB",
  "JAVA_TOOL_OPTIONS",
  "JDK_JAVA_OPTIONS",
  "_JAVA_OPTIONS",
  "CLASSPATH",
  "GCONV_PATH",
  "GETCONF_DIR",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "ALL_PROXY",
  "NO_PROXY",
  "SSL_CERT_FILE",
  "SSL_CERT_DIR",
  "CURL_CA_BUNDLE",
  "REQUESTS_CA_BUNDLE",
  "BASH_FUNC",
  "ENV_SUPATH",
  "SSLKEYLOGFILE",
  "GIT_CONFIG",
]);
export function assertCustomApiRuntimeEnvironmentName(name: string): void {
  assertCustomApiEnvironmentName(name);
  if (
    reserved.has(name) ||
    /^(?:__|LD_|DYLD_|PYTHON|NODE_|OPENSSL_|DOCKER_|E2B_|KOKORO_|BASH|GIT_CONFIG_)/u.test(
      name,
    )
  )
    throw new FailedPreconditionError(
      "Custom API runtime environment name reserved",
    );
}
