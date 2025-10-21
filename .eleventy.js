const eleventySass = require("eleventy-sass");

const sass_options = {
  sass: {
    loadPaths: ["./node_modules/design-system/system"],
    style: "expanded",
    sourceMap: true,
  },
  defaultEleventyEnv: "development",
};

module.exports = function (eleventyConfig) {
  eleventyConfig.setBrowserSyncConfig({
    files: "./_site/css/**/*.css",
  });

  eleventyConfig.addPlugin(eleventySass, sass_options);

  // Copy CSV file to output directory for download
  eleventyConfig.addPassthroughCopy({
    "_data/services.csv": "data/services.csv",
  });
};
