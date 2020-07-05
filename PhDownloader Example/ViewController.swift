//
//  ViewController.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import UIKit
import RxSwift

struct Item {
  let request: PhDownloadRequest
  var state: PhDownloadState
}

class ViewController: UIViewController {

  @IBOutlet weak var tableView: UITableView!

  private let downloader: PhDownloader = try! PhDownloaderFactory.downloader(with: .init(maxConcurrent: 2, throttleProgress: .milliseconds(500)))

  private let disposeBag = DisposeBag()

  private var items: [Item] = (0..<100).map { i in
      .init(
        request: .init(
          identifier: String(i),
          url: URL(string: "https://file-examples.com/wp-content/uploads/2017/11/file_example_MP3_5MG.mp3")!,
          fileName: "test_file_\(i)",
          savedDir: FileManager.default
            .urls(for: .documentDirectory, in: .userDomainMask)
            .first!
            .appendingPathComponent("downloads", isDirectory: true)
        ),
        state: .undefined
      )
  }

  override func viewDidLoad() {
    super.viewDidLoad()

    self.tableView.dataSource = self
    self.tableView.delegate = self

    self.downloader
      .observeState(by: self.items.map { $0.request.identifier })
      .subscribe(onNext: { [weak self] states in
        guard let self = self else { return }

        self.items = self.items.map { item in
          var copy = item
          copy.state = states[item.request.identifier] ?? .undefined
          return copy
        }
        self.tableView.reloadData()
        print(states)
      })
      .disposed(by: self.disposeBag)

    DispatchQueue.main.asyncAfter(deadline: .now() + 5) {
      self.items.map { $0.request }.forEach { request in
        self.downloader
          .enqueue(request)
          .subscribe()
          .disposed(by: self.disposeBag)

      }
    }
  }
}

extension ViewController: UITableViewDataSource, UITableViewDelegate {
  func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
    items.count
  }

  func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
    let cell = tableView.dequeueReusableCell(withIdentifier: "DownloadCell", for: indexPath)
    let item = self.items[indexPath.row]

    cell.textLabel?.text = item.request.url.absoluteString
    cell.detailTextLabel?.text = "\(item.state)"

    return cell
  }
}

