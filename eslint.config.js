import tseslint from 'typescript-eslint';

export default tseslint.config(
  // Base recommended rules
  ...tseslint.configs.recommended,

  // Global ignores
  {
    ignores: [
      'dist/**',
      'build/**',
      'node_modules/**',
      'coverage/**',
      'jest.config.js',
    ],
  },

  // TypeScript source files
  {
    files: ['**/*.ts'],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // Enforce no unused variables (error for vars, warn for args prefixed with _)
      '@typescript-eslint/no-unused-vars': ['error', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
      }],

      // Allow explicit any — we use it deliberately in some places
      '@typescript-eslint/no-explicit-any': 'warn',

      // Require explicit return types on exported functions
      '@typescript-eslint/explicit-function-return-type': ['warn', {
        allowExpressions: true,
        allowTypedFunctionExpressions: true,
        allowHigherOrderFunctions: true,
      }],

      // No floating promises
      '@typescript-eslint/no-floating-promises': 'error',

      // Prefer const
      'prefer-const': 'error',

      // No var
      'no-var': 'error',

      // Consistent type imports
      '@typescript-eslint/consistent-type-imports': ['error', {
        prefer: 'type-imports',
        fixStyle: 'inline-type-imports',
      }],
    },
  },

  // Relaxed rules for test files
  {
    files: ['tests/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
    },
  },

  // Relaxed rules for scripts (experimental / throwaway)
  {
    files: ['scripts/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/no-unused-vars': 'warn',
    },
  },
);
