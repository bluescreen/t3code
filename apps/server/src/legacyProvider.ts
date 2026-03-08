export function normalizeLegacyProviderValue(value: unknown): unknown {
  if (value === "claude") {
    return "denkvis";
  }
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeLegacyProviderValue(entry));
  }
  if (value && typeof value === "object") {
    const next: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      next[key] =
        (key === "provider" || key === "providerName") && entry === "claude"
          ? "denkvis"
          : normalizeLegacyProviderValue(entry);
    }
    return next;
  }
  return value;
}
