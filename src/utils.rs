use indicatif::{ProgressBar, ProgressStyle};

pub fn generate_hex_strings(n_digits: u32) -> Vec<String> {
    (0..16_u64.pow(n_digits))
        .map(|i| format!("{i:0width$x}", width = n_digits as usize))
        .collect()
}

pub fn make_progress_bar(total: Option<u64>) -> ProgressBar {
    let pb;
    let sty;
    match total {
        Some(total) => {
            pb = ProgressBar::new(total);
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} [{bar:40.cyan/blue}] {pos:>7}/{len:7} [{elapsed_precise}<{eta_precise} {per_sec:.green}] {msg}"
            )
            .unwrap()
            .progress_chars("█▓▒░");
        }
        None => {
            pb = ProgressBar::new_spinner();
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} {pos:>7} [{elapsed_precise} {per_sec:.green}]",
            )
            .unwrap()
            .progress_chars("█▓▒░");
        }
    }
    pb.set_style(sty);
    pb
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn handle_input() {
    // input
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    if input.trim() == "q" {
        std::process::exit(0);
    }
}
